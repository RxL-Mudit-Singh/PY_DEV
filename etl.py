import json
import cx_Oracle as cx
from flatten_json import flatten
import time
from multiprocessing import Process,Pool
from  threading import  Thread
import boto3 as bt
import logging
import logging_json as lg
import jsonlines
# %(threadName)s
# %(name)s
# %(levelname)s
# %(asctime)s 
################################################################################################
multikey = []
################################################################################################

#! Fetching sqs from dump msg
def recieve_message(url)->list:
    lst = []
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.receive_message(
        QueueUrl = url,
        MaxNumberOfMessages=3,
        WaitTimeSeconds=0
    )
    val = response['Messages'][0]['Body']
    val2 = json.loads(val)
    bucket_name = val2['Records'][0]['s3']['bucket']['name']
    fileKey = val2['Records'][0]['s3']['object']['key']
    lst.append(bucket_name)
    lst.append(fileKey)
    return lst


################################################################################################

def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}

#! Removing the ArrayElem
def pre_proc(data):
    blank_key_data = []
    for k,v in data.items():
        if isinstance(v,dict):
            if 'ArrayElem' in v.keys():
                data[k]=v['ArrayElem']
            if isinstance(data[k],list):
                if len(data[k][0]) == 0:
                    blank_key_data.append(k)
        
    # for k1,v1 in data.items():
    #     if isinstance(v1,list):
    #         if len(v1[0]) == 0:
    #             blank_key_data.append(k1)
    # {'c_actions_addl':[{}]}
    for key_to_be_deleted in blank_key_data:
        del data[key_to_be_deleted]
    # print(blank_key_data)
    # print(data)
    return data
    
    
#! Flattening the dictionary 
def flatten_list(d):
    try:
        key, lst = next((k, v) for k, v in d.items() if isinstance(v, list))
    except (StopIteration, AttributeError):
        return [flatten(d,'.')]
    return [flatten({**d, **{key: v}},'.') for record in lst for v in flatten_list(record)]

def generate_inserts(table, data):
    jsons = flatten_list(data)
    statements = []
    ins = jsons[0]
    ins = f"INSERT INTO {table} ({','.join(ins.keys())}) VALUES ({','.join([':'+str(v) for v in range(len(ins.keys()))])})"
    val_array = []
    for i in jsons:
        cols = i.keys()
        values = i.values()
        val_array.append(tuple(values))
    statements.append(ins)
    statements.append(val_array)
    return statements


#! Creating insert statements
def return_all_inserts(dictionary)->dict:
    generic_keys = ['TENANT_ID','CASE_ID','VERSION_NUM']
    inserts = {}
    dictionary2 = pre_proc(dictionary)
    for i in dictionary2:
        if i not in generic_keys:
            t1 = include_keys(dictionary, generic_keys+[i])
            for j in t1:
                if isinstance(t1[j],dict):
                    if 'ArrayElem' in t1[j].keys():
                        t1[j] = t1[j]['ArrayElem']
            inserts[i] = tuple(generate_inserts(i,t1))
    return inserts


################################################################################################
def push_data(dictionary1, cnxn_pool):
    global failures
    dictionary2 = return_all_inserts(dictionary1)
    print(dictionary2)
    st1 = time.time()
    cnxn = cnxn_pool.acquire()
    cursor = cnxn.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY hh24:mi:ss'")
    print("""INCURSION :)(: STARTED \n""")
    failed_insert = {}
    for key,val  in dictionary2.items(): 
        val_array = val[1]
        print(type(val[0]))
        print(val[0])
        print(val[1])
        try:
            cursor.executemany(val[0],val_array)
        except cx.DatabaseError as e:
            multikey.append({"f_id" :f"{dictionary1['TENANT_ID']}^{dictionary1['CASE_ID']}^{dictionary1['VERSION_NUM']}","tables":f"{key}-> {e}"})
            failed_insert[key] = val
            print("Error is->  ",e)
        break
        # else:
            # print("Insert Done")
    if len(failed_insert) == 0:
        # cnxn.commit()
        print("All Inserts Committed")
    else:
        cnxn.rollback()
        failures.append(failed_insert)
        print("Rollback Done")
    cnxn_pool.release(cnxn)

################################################################################################

# ! Splitting the file into multiple chunk_data 
def split_file(a,n):
    k,m = divmod(len(a),n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load(file)->list:
    with open(file,'r') as fp:
        string = fp.read()
        o = json.loads(string)
        line_chunk = split_file(o,1)
        print('chunk created')
        return line_chunk


if __name__ == "__main__":
    st = time.time()
    # try:
    #     arr = recieve_message("https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue")
    #     s3 = bt.client('s3')
    #     s3.download_file(Bucket=arr[0],Key=arr[1],Filename='C:/DEV/PY_DEV/output.json')
    # except Exception as e:
    #     logging.error("Error downloading file")
    failures = []
    cnxn_pool = cx.SessionPool('PVD_MART_30AUG','rxlogix','10.100.22.99:1521/PVSDEVDB',min=12,max=15,encoding='UTF-8')
    chunk_data = load('jsons/msg3.json')
    threads = []
    for i in chunk_data[0]:
        thread = Thread(target=push_data, args=(i,cnxn_pool),daemon=True)
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()
        
    # while True:
    #     if any([t.is_alive() for t in threads]):
    #         print("Threads running")
    #         time.sleep(10)
    #     else:
    #         break
    cnxn_pool.close()
    #? Uploading Failed Inserts to S3
    # with open('jsons/failues.json', 'w') as F:
    #     J = json.dumps(failures, encoding = 'utf-8',indent =1)
    #     F.write(J)
    #     s3.upload_file(F,Bucket=arr[0])
    with open('logs.json','w',encoding='utf-8') as logs:
                G = json.dumps(multikey,indent=1)
                logs.write(G)
    print(f" length of failures : {len(failures)}")