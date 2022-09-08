import asyncio
from sched import scheduler
from urllib import response
from attr import Attribute
import boto3 as bt
import json
import cx_Oracle as cx
import tornado.web as tw
import tornado.ioloop
from tornado.escape import json_decode
import json
import requests
from tornado.ioloop import IOLoop
from apscheduler.schedulers.tornado import TornadoScheduler


def create_queue():
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.create_queue(
        QueueName = "bk_queue",
        Attributes={
            "DelaySeconds":"0",
            "VisibilityTimeout":"60",
            "FifoQueue":True
        }
    )
    print(f"Queue created : {'QueueName'}")
    # print(response)


def get_queue_url(name):
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.get_queue_url(
        QueueName = name,
    )
    return response["QueueUrl"]



def send_message():
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    message = {"name" : "Aditya" , "age" : 21, "location" : "Delhi"}
    response = sqs_client.send_message(
        QueueUrl = "https://ap-south-1.queue.amazonaws.com/884379823401/bk_queue",
        MessageBody = json.dumps(message)
    )
    return response


def recieve_message(url):
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.receive_message(
        QueueUrl = url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )
    return response


def delete_message(reciept_handle,url):
    sqs_client = bt.client("sqs",region_name="ap-south-1")
    response = sqs_client.delete_message(
        QueueUrl = url,
        ReceiptHandle =  reciept_handle,
    )
    print("Message Deleted")
    
def process_msg(QueueName)->list:
    QueueUrl = get_queue_url(QueueName)
    response =  recieve_message(QueueUrl)
    lst = []
    for message in response.get("Messages",[]):
        message_body = message["Body"]
        lst.append(message_body)
    return lst


class reqHandler(tornado.web.RequestHandler): 
    def post(self):
        print(self.request.body)
        byte_value2 = json.loads(self.request.body) 
        s = json.dumps(byte_value2,indent=4)
        cdns = cx.makedsn('10.100.22.99','1521',service_name='PVSDEVDB')
        cnxn = cx.connect(user=r'PVS_DB_55JULY', password='rxlogix', dsn=cdns)
        c = cnxn.cursor()
        c.execute(f"insert into tor_test(NAME,AGE,LOCATION) values('{byte_value2['name']}','{byte_value2['age']}','{byte_value2['location']}')")
        cnxn.commit()
        cnxn.close()  
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')
           
           
async def schedule():
    # scheduler = TornadoScheduler()
    # scheduler.add_job(lambda:recieve_message("https://ap-south-1.queue.amazonaws.com/884379823401/bk_queue"),'interval', seconds=10)
    # scheduler.start()
    # try:
    #     IOLoop.current().start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()
    #     print("Scheduler Down")
    resp = recieve_message("https://ap-south-1.queue.amazonaws.com/884379823401/bk_queue")
    print(resp)
    
    
            
           
async def main():
    asyncio.ensure_future(schedule())
    application = tornado.web.Application([
        (r"/", reqHandler),
    ])
    application.listen(8001)
    print("On port 8001")
    await asyncio.sleep(25)  
    
# def main():
#     application = tornado.web.Application([
#         (r"/", reqHandler),
#     ])
#     application.listen(8001)
#     print("On port 8001")
             
if __name__=="__main__":
    asyncio.run(main())
    try:
        IOLoop.instance().start()
    except(KeyboardInterrupt,SystemExit):
        print("Bye !")
    
    # scheduler = TornadoScheduler()
    # scheduler.add_job(main,'interval',seconds= 10)
    # scheduler.add_job(lambda:recieve_message("https://ap-south-1.queue.amazonaws.com/884379823401/bk_queue"),'interval', seconds=10)
    # scheduler.start()
    # try:
    #     IOLoop.current().start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()
    #     print("Scheduler Down")
         
    
    


    # application.listen(8001)
    # print("On Port 8000")
    # scheduler = TornadoScheduler()
    # scheduler.add_job(lambda:recieve_message("https://ap-south-1.queue.amazonaws.com/884379823401/bk_queue"),'interval', seconds=15)
    # scheduler.start()
    # try:
    #     IOLoop.instance().start()
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
#     app = tornado.web.Ap plication(
#         [
#            (r"/",reqHandler)
#         ]
#     )
# x = 8002 
# app.listen(x)
# print(f"On port {x}")
# tornado.ioloop.IOLoop.current().start()