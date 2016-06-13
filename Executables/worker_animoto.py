"""
    Remote Worker for Animoto Jobs
    The code is implemented to launch the animoto clone application
    which converts images to video
    It requires S3 bucket already created to put onto it the videos generated
"""

import boto.sqs
from boto.sqs.message import RawMessage
from boto.s3.connection import S3Connection, Location
import time
import boto.dynamodb
import boto.s3
import argparse
import json
from subprocess import call
import os
import math
from filechunkio import FileChunkIO

# Code to Upload video to s3
def upload_video_s3(sqs_response_q, config):

    S3_conn = boto.s3.connect_to_region("us-east-1",
                aws_access_key_id = config["AWSAccessKeyId"],
                aws_secret_access_key = config["AWSSecretKey"])

    #print("Connected to S3 ...")

    # Create a New Bucket, and check if the Bucket Name is already taken
    #try:
    #    S3_conn.create_bucket('animoto_video_storage_shalin')
    #    print("Bucket Created ...")
    #except:
    #    print("Already Existing Bucket .. Please choose different Bucket")
        #print("Error")

    S3Bucket = S3_conn.get_bucket('animoto_video_storage_shalin')
    #print("Connected to Bucket")

    #### Code Reference http://boto.cloudhackers.com/en/latest/s3_tut.html

    # Get video files Path Info
    video_paths = []

    #for every file in the directoy find the file which has the .MKV or .mkv extension
    for video in os.listdir("/home/ubuntu/TaskExecutionFramework/"):
        if os.path.isfile(video):
            if video.split('.')[1] == 'MKV' or video.split('.')[1] == 'mkv':
                video_paths.append("/home/ubuntu/TaskExecutionFramework/" + os.path.basename(video))

    #Now to put videos into S3

    for video_path in video_paths:

        #find the video size
        source_size = os.stat(video_path).st_size

        # Create a multipart upload request
        mp = S3Bucket.initiate_multipart_upload(os.path.basename(video_path))
        #print(os.path.basename(video_path))

        # Use a chunk size of 50 MiB
        chunk_size = 5242880
        chunk_count = int(math.ceil(source_size / chunk_size))

        # Send the file parts, using FileChunkIO
        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(video_path, 'r', offset = offset, bytes = bytes) as fp:
                mp.upload_part_from_file(fp, part_num = i+1)

        # Complete uploading the parts entirely
        mp.complete_upload()

        # get the Key for S3 video using the video name for fetching URL
        file_key = S3Bucket.get_key(os.path.basename(video_path))

        url = file_key.generate_url(600)    # URL is valid for 10 minutes

        #write onto the Response Queue the URL of the Video
        m = RawMessage()
        m.set_body(url)
        sqs_response_q.write(m)

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
    i = 0
    while (flag == 1):
        if len(rs) > 0:
            count_empty = 0
            #Extract Message Features
            m = rs[0]
            task_url = m.get_body().split(" ")[0]
            #task_fetched = m.get_body().split(" ")[0] + task_sleep_time
            task_ID = int(m.get_body().split(" ")[1])

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
                item_data = {'Tasks': task_url}
                item = Dy_table.new_item(
                    hash_key = task_ID,
                    attrs = item_data
                )
                item.put()

                # Extract URL Images task from QUEUE
                try:
                    # print("Deleting from SQS ...")
                    print("Generating Video from Images")
                    call('sh exec_images_download.sh {} >> ~/Log{}.txt'.format(str(task_ID).zfill(3),str(task_ID).zfill(3)),shell=True)
                    sqs_q.delete_message(rs[0])
                    i+=1

                    #call the S3 component to store the video and return video storage URL
                    upload_video_s3(sqs_response_q,config)
                except:
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
    #print("Total Duplicate Tasks Detected: ",count_dupli)
