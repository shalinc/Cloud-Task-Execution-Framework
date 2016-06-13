"""
    This is a Client file for remote execution which interacts with SQS
    The client help will be provided by command python client_SQS.py -h
"""

import argparse
import time
import boto.sqs
from boto.sqs.message import RawMessage, Message
import json
import boto.dynamodb
import os

# Client Class
class client(object):

    # Ctor
    def __init__(self, q_name, workload_file):
        self.q_name = q_name
        self.workload_file = workload_file

    # Methods
    def fetch_tasks(self):
        try:
            file_tasks = open(self.workload_file,'r').readlines()
            #print(file_tasks)
        except IOError as e:
            print("ERROR OCCURED: ",e)
        return file_tasks, len(file_tasks)

    def send_tasks_queue(self, tasks, tasks_queue):
        print("Establishing a Connection with SQS Queue: ",self.q_name+" ...")
        print("Connection Established !!!")
        print("Putting Messages in the Queue ...")
        i=0
        for task in tasks:
            #tasks_queue.send_message(MessageBody=str(task.strip()+" "+str(i)))
            m = Message()
            m.set_body(str(task.strip()+" "+str(i)))
            tasks_queue.write(m)
            i+=1
        #print("All Messages are in QUEUE for REMOTE WORKERS to fetch")
        return tasks_queue
        #print("All Messages are in QUEUE for REMOTE WORKERS to fetch")

    #def createThreadPool(self,N):
    #    local_workers = [localWorker(self.task_queue, self.response_queue) for th in range(N)]
    #    return local_workers


# main
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-s',metavar="QUEUE NAME",type=str,help="the Name of the QUEUE (LOCAL/SQS_QUEUE_NAME)", required=True)
    parser.add_argument('-w',metavar="WORKLOAD FILE",type=str,help="Path of the Workload File", required=True)
    args = parser.parse_args()

    # Load the AWS Configurations
    config_file = open("config.json",'r')
    config = json.load(config_file)

    #store the QUEUE NAME for Checking
    q_name = args.s
    if not q_name == "TasksQueue":
        print("INVALID SQS QUEUE NAME")
        exit(0)

    # name of the workload_file
    #workload_file = args.w
    workload_file = os.getcwd()+"/Workloads/"+args.w
    response_file = os.getcwd()+"/ResponseLogs/"+args.w

    # Client Class Object Creation
    client = client(q_name, workload_file)

    # Fetch the tasks from the Workload File
    tasks, no_of_tasks = client.fetch_tasks()

    # Connect to SQS Queue
    Queue_conn = boto.sqs.connect_to_region("us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])
    sqs_q = Queue_conn.get_queue(q_name)
    sqs_resp_q = Queue_conn.get_queue('ResponseQueue')

    #Put the tasks onto a QUEUE
    start = time.time()
    task_queue = client.send_tasks_queue(tasks, sqs_q)

    while(no_of_tasks != sqs_resp_q.count()):
        pass

    end = time.time()
    print("Time taken to Execute all Msgs in SQS:", str(end-start)+" sec")

    # writing to Log file
    # Adding response to the output_file
    file = open(response_file+"_Log.txt",'w+')

    # Get msgs from SQS
    rs = sqs_resp_q.get_messages()
    while len(rs) > 0:
        m = rs[0]
        file.write(str(m.get_body())+"\n")
        rs = sqs_resp_q.get_messages()
    file.close()

    print("Logs written Successfully in",response_file+"_Log.txt")

    #Connect to DynamoDB
    # DyDB_conn = boto.dynamodb.connect_to_region("us-east-1",
    #         aws_access_key_id = config["AWSAccessKeyId"],
    #         aws_secret_access_key = config["AWSSecretKey"])
    #
    # Dy_table = DyDB_conn.get_table('TasksData')
    # print("Connected to DynamoDB")
    #
    # rs = sqs_q.get_messages()
    # while len(rs) > 0:
    #     m = rs[0]
    #     task_sleep_time = m.get_body().split(" ")[1]
    #     task_fetched = m.get_body().split(" ")[0] + task_sleep_time
    #     task_ID = int(m.get_body().split(" ")[2])
    #
    #     #print(type(task_ID), task_ID)
    #     try:
    #         val = Dy_table.get_item(hash_key=task_ID)
    #         print("Task Already Executed",val)
    #         sqs_q.delete_message(rs[0])
    #         rs = sqs_q.get_messages()
    #     except:
    #         print("Key Not found in Table .. Adding Now")
    #         item_data = {'Tasks': task_fetched}
    #         item = Dy_table.new_item(
    #             hash_key = task_ID,
    #             attrs = item_data
    #         )
    #         item.put()
    #         # Extract SLEEP task time from QUEUE
    #         sleepTime = task_sleep_time
    #         # Conversion to MilliSeconds
    #         time.sleep(int(sleepTime)/1000.0)  #Add divide by 1000
    #         #print("Deleting from SQS ...")
    #         sqs_q.delete_message(rs[0])
    #         rs = sqs_q.get_messages()
    #         continue
