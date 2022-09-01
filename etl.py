# ? insert stat dump
import json
import cx_Oracle as cx
from flatten_json import flatten
# from concurrent.futures import ThreadPoolExecutor
import time
from multiprocessing import Process,Pool
from  threading import Lock, Thread
def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}

def pre_proc(data):
        print(type(data))
        for k,v in data.items():
            if isinstance(v,dict):
                if 'ArrayElem' in v.keys():
                # for k1,v1 in v.items():
                    data[k]=v['ArrayElem']
        return data
    
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


def push_data(dictionary1, cnxn_pool):
    print("Thread created")
    global failures
    dictionary2 = return_all_inserts(dictionary1)
    st1 = time.time()
    cnxn = cnxn_pool.acquire()
    cursor = cnxn.cursor()
    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY hh24:mi:ss'")
    print("""INCURSION :)(: STARTED""")
    failed_insert = {}
    for key,val  in dictionary2.items(): 
        val_array = val[1]
        try:
            cursor.executemany(val[0],val_array)
        except cx.DatabaseError as e:
            # print(e)
            failed_insert[key] = val
            # break
        # else:
            # print("Insert Done")
    if len(failed_insert) == 0:
        cnxn.commit()
        print("All Inserts Committed")
    else:
        cnxn.rollback()
        failures.append(failed_insert)
        print("Rollback Done")
    # print("data chunk inserted")
    cnxn_pool.release(cnxn)
    
        
# if __name__ == '__main__':
#     with open('jsons/msg1.json','r') as msg1:
#         string = msg1.read()
#         D = json.loads(string)
#         # dictionary = return_all_inserts(D)
#         push_data(D)
#     cnxn_pool.close()
    
def split_file(a,n):
    k,m = divmod(len(a),n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load(file)->list:
    with open(file,'r', encoding="utf8") as fp:
        string = fp.read()
        o = json.loads(string)
        line_chunk = split_file(o,1)
        print('chunk created')
        return line_chunk


if __name__ == "__main__":
    st = time.time()
    failures = []
    cnxn_pool = cx.SessionPool('PVD_MART_30AUG','rxlogix','10.100.22.99:1521/PVSDEVDB',min=12,max=15,encoding='UTF-8')
    chunk_data = load('jsons/msg1.json')
    print(len(chunk_data))
    print(type(chunk_data))

    # with ThreadPoolExecutor(max_workers=10) as threads:
    #     threads.map(push_data,chunk_data)
    # cnxn_pool.close()
    threads = []
    for i in chunk_data[0]:
        thread = Thread(target=push_data, args=(i,cnxn_pool,),daemon=True)
        # thread.daemon = True
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
    print(f" length of failures : {len(failures)}")
    # et = time.time()
    # print(et-st)