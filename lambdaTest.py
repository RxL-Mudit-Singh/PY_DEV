import boto3 as bt
import json
import cx_Oracle as cx
import tornado.web as tw
import json
import requests
import cx_Oracle as cx
import pandas as pd
from multiprocessing import Process,Pool
from tornado.ioloop import IOLoop
from apscheduler.schedulers.tornado import TornadoScheduler
import time
from concurrent.futures import ThreadPoolExecutor, as_completed



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



 
def post(lst):
    cdns = cx.makedsn('10.100.22.99','1521',service_name='PVSDEVDB')
    cnxn = cx.connect(user=r'PVS_DB_55JULY', password='rxlogix', dsn=cdns)
    # print(cnxn)
    c = cnxn.cursor()
    # print(lst)
    for data in lst:
        # print(data)
        # print(type(data))
        # print(data['first_name'])
        try:
            # print(data[0])
            # print(f"insert into TEST_DATA(ID,FIRST_NAME,EMAIL,GENDER,ADDRESS) values({data['id']},  '{data['first_name']}',  '{data['email']}',  '{data['gender']}','{data['address']}')")
            c.execute(f"insert into TEST_DATA2(ID,FIRST_NAME,EMAIL,GENDER,ADDRESS) values({data['id']},  '{data['first_name']}',  '{data['email']}',  '{data['gender']}','{data['address']}')")
        except Exception as e:
            # cnxn.roll_back()
            print(e)
        cnxn.commit()
    cnxn.close()  
           
    
def split(a,n):
    k,m = divmod(len(a),n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load(file)->list:
    with open(file,'r', encoding="utf8") as fp:
        string = fp.read()
        o = json.loads(string)
        line_chunk = split(o,1000)
        return line_chunk


# if __name__ == "__main__":
#     st = time.time()
#     arr = recieve_message("https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue")
#     s3 = bt.client('s3')
#     s3.download_file(Bucket=arr[0],Key=arr[1],Filename='C:/DEV/PY_DEV/output.json')
#     chunk_data = load('output.json')
#     with ThreadPoolExecutor(max_workers=10) as threads:
#         threads.map(post,chunk_data)
#     et = time.time()
#     print(et-st)
           
            



if __name__ == "__main__":
    st = time.time()
    arr = recieve_message("https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue")
    s3 = bt.client('s3')
    s3.download_file(Bucket=arr[0],Key=arr[1],Filename='C:/DEV/PY_DEV/output.json')
    chunk_data = load('output.json')
    pool = Pool()
    pool.map(post,chunk_data)
    et = time.time()
    print(et-st)
           