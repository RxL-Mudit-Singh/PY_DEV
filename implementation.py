import asyncio
from datetime import datetime
import json
from tornado.ioloop import IOLoop
from apscheduler.schedulers.tornado import TornadoScheduler
import boto3 as bt
import numpy as np
import pickle as pk
from multiprocessing import Process,Pool
import time

def GetMessage():
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.receive_message(
        QueueUrl = "https://sqs.ap-south-1.amazonaws.com/884379823401/testQueue",
        MaxNumberOfMessages=3,
        WaitTimeSeconds=5
    )
    return response

def multiplePool():
    pool = Pool(2)
    process = [pool.apply_async(GetMessage) for i in range(5)]
    result = [p.get() for p in process]
    dictVal = {}
    for msg in result:
        for msg_body in msg['Messages']:
            json_obj = json.dumps({msg_body['MessageId']:msg_body['Body']},indent=4)
            with open('sample.json','a') as outfile:
                outfile.write(json_obj) 
                outfile.write('\n')
            
            
            with open('sample.json', 'r+') as file:
                content = file.read()
                file.seek(0)
                content.replace("\'", "")
                file.write(content)        
    
if __name__ == '__main__':
    scheduler = TornadoScheduler()
    scheduler.add_job(multiplePool,'interval', seconds=30)
    scheduler.start()
    try:
        IOLoop.instance().start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
    # multiplePool()