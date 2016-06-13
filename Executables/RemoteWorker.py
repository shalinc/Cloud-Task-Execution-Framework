"""
    Remote Worker
    This is code for implementation Remote Worker
    The code help can be found by typing RemoteWorker.py -h
"""

import boto.sqs
from boto.sqs.message import RawMessage
import time
import boto.dynamodb
import argparse
import json


# main
if __name__ == "__main__":

    # Command Line Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s",metavar="QUEUE NAME",help="Name of the SQS QUEUE",required=True)
    parser.add_argument("-t",metavar="Number of Workers",help="Number of Workers required", required=True)

    args = parser.parse_args()
    q_name = args.s

    # Load the AWS Configurations
    config_file = open("config.json",'r')
    config = json.load(config_file)

    # Connect to SQS Task and Response Queue
    Queue_conn = boto.sqs.connect_to_region("us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])
    try:
        sqs_q = Queue_conn.get_queue(q_name)
    except:
        print("Error in Queue Name .. Please Check again ..")

    sqs_response_q = Queue_conn.get_queue('ResponseQueue')

    #Connect to DynamoDB
    DyDB_conn = boto.dynamodb.connect_to_region("us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])

    Dy_table = DyDB_conn.get_table('TasksData')
    #print("Connected to DynamoDB ...")

    #start = time.time()

    # get all the messages from the SQS Queue
    # Check for Duplicates here using DynamoDB

    # get messages
    rs = sqs_q.get_messages()
    count_dupli, count_empty = 0, 0
    flag = 1

    # Loop till SQS Queue is Empty i.e. there are no tasks left in Queue
    # Wait for 10 sec's to check if there are any tasks left, else terminate
    while (flag == 1):
        if len(rs) > 0:
            count_empty = 0

            #Extract Message Features
            m = rs[0]
            task_sleep_time = m.get_body().split(" ")[1]
            task_fetched = m.get_body().split(" ")[0] + task_sleep_time
            task_ID = int(m.get_body().split(" ")[2])

            #print(type(task_ID), task_ID)
            # Check if Task Already Executed by other worker
            try:
                val = Dy_table.get_item(hash_key=task_ID)
                #print("Task Already Executed",val)
                sqs_q.delete_message(rs[0])
                rs = sqs_q.get_messages()
                count_dupli+=1
            except:
                #print("Executing Task Now ...")
                item_data = {'Tasks': task_fetched}
                item = Dy_table.new_item(
                    hash_key = task_ID,
                    attrs = item_data
                )
                item.put()

                # Extract SLEEP task time from QUEUE
                sleepTime = task_sleep_time

                try:
                    # Conversion to MilliSeconds
                    time.sleep(int(sleepTime)/1000.0)  #Add divide by 1000
                    #print("Deleting from SQS ...")
                    sqs_q.delete_message(rs[0])

                    #Adding Response to SQS Queue
                    m = RawMessage()
                    m.set_body(0)
                    sqs_response_q.write(m)
                    rs = sqs_q.get_messages()
                except:
                    #Adding Response to SQS Queue
                    m = RawMessage()
                    m.set_body(1)
                    sqs_response_q.write(m)
                    rs = sqs_q.get_messages()

                continue
        else:
            if count_empty < 10:
                flag = 1
                count_empty+=1
                time.sleep(1)
                print("...Polling for Messages in Queue ... waiting 1 sec ...")
                rs = sqs_q.get_messages()
            else:
                flag = 0

    #end = time.time()
    #print("Time Taken for Worker: ",str(end-start)+" sec")
    print("Total Duplicate Tasks Detected: ",count_dupli)