import boto3 as bt
import json
import cx_Oracle as cx
import tornado.web as tw
import json
# import requests
import pandas as pd
from multiprocessing import Process,Pool
from tornado.ioloop import IOLoop
# from apscheduler.schedulers.tornado import TornadoScheduler
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from timeit import default_timer as dt
import time
import json
from flatten_json import flatten
from datetime import datetime


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

def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}  

def file_transform_1(file):
    with open(file,'r+',encoding='utf-8') as d:
        string = d.read()
        data  = json.loads(string)
        # data = include_keys(data, ['TENANT_ID','CASE_ID','VERSION_NUM','C_ACTIONS_ADDL','C_ASSESS_ADDL'])
        for k,v in data.items():
            if isinstance(v,dict):
                if 'ArrayElem' in v.keys():
                # for k1,v1 in v.items():
                    data[k]=v['ArrayElem']
        g = json.dumps(data)
        with open('jsons/sample.json','w',encoding='utf-8') as f:
            g = json.dumps(data,indent=1)
            f.write(g)
            
def flatten_list(d):
    try:
        key, lst = next((k, v) for k, v in d.items() if isinstance(v, list))
    except (StopIteration, AttributeError):
        return [flatten(d,'.')]
    return [flatten({**d, **{key: v}},'.') for record in lst for v in flatten_list(record)]


def generate_inserts(table, data):
    jsons = flatten_list(data)
    # print(jsons)
    statements = []
    for i in jsons:
        cols = i.keys()
        values = i.values()
        ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES {tuple(values)}".replace('None','null')
        statements.append((ins))
    return statements


def return_all_inserts(dictionary):
    generic_keys = ['TENANT_ID','CASE_ID','VERSION_NUM']
    inserts = {}
    for i in dictionary:
        if i not in generic_keys:
            t1 = include_keys(dictionary, generic_keys+[i])
            for j in t1:
                if isinstance(t1[j],dict):
                    if 'ArrayElem' in t1[j].keys():
                        t1[j] = t1[j]['ArrayElem']
            inserts[i] = generate_inserts(i,t1)
    return inserts

with open('jsons/msg1.json','r',encoding='utf-8') as X:
    start = dt()
    string = X.read()
    D = json.loads(string)
    print(type(D))
    # print(return_all_inserts(D))
    with open('read.json','w') as T:
        # print(return_all_inserts(D))
        # T.write(str(return_all_inserts(D)))
        g = json.dumps(return_all_inserts(D),indent=1)
        T.write(g)
    end = dt()
    # print(end-start)
# cdns = cx.makedsn('10.100.22.99','1521',service_name='PVSDEVDB')
# cnxn_pool = cx.SessionPool('PVS_DB_55JULY','rxlogix','10.100.22.99:1521/PVSDEVDB',min=10,max=15,encoding='UTF-8')
# print(cnxn_pool)
# def post(lst):
#     print("Thread Started  ->",time.time())
#     print(lst)
#     print("""
#           |
#           |
#           |
#           """)
    
#     for data in lst:
#         try:
#             cnxn = cnxn_pool.acquire()
#             cursor = cnxn.cursor()
#             cursor.execute(f"insert into TEST_DATA2(ID,FIRST_NAME,EMAIL,GENDER,ADDRESS) values({data['id']},  '{data['first_name']}',  '{data['email']}',  '{data['gender']}','{data['address']}')")
#             cnxn.commit()
#             cnxn_pool.release(cnxn)
#             # print("completed")
#         except Exception as e:
#             cnxn.roll_back()
#             print(e)
#             cnxn_pool.release(cnxn)
#     print("data chunk inserted")
        
      
           
    
# def split_file(a,n):
#     k,m = divmod(len(a),n)
#     return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

# def load(file)->list:
#     with open(file,'r', encoding="utf8") as fp:
#         string = fp.read()
#         o = json.loads(string)
#         line_chunk = split_file(o,100)
#         return line_chunk


# if __name__ == "__main__":
#     st = time.time()
#     arr = recieve_message("https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue")
#     s3 = bt.client('s3')
#     s3.download_file(Bucket=arr[0],Key=arr[1],Filename='C:/DEV/PY_DEV/output.json')
#     chunk_data = load('output.json')
#     with ThreadPoolExecutor(max_workers=8) as threads:
#         threads.map(post,chunk_data)
#     cnxn_pool.close()
#     et = time.time()
#     print(et-st)
           
            



# # if __name__ == "__main__":
# #     st = time.time()
# #     arr = recieve_message("https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue")
# #     s3 = bt.client('s3')
# #     s3.download_file(Bucket=arr[0],Key=arr[1],Filename='C:/DEV/PY_DEV/output.json')
# #     chunk_data = load('output.json')
# #     pool = Pool()
# #     pool.map(post,chunk_data)
# #     cnxn_pool.close()
# #     et = time.time()
# #     print(et-st)
           