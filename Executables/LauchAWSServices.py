"""
    This program is used to launch:
    1. SQS QUEUE
    2. DYNAMO DB
    3. T2 Micro Instance
    4. S3 Bucket
"""

import boto.sqs
import boto.ec2
import boto.dynamodb
import json
import argparse
import boto.s3


# main
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("service",help="Which Amazon Service to Launch (sqs/t2m/dydb/s3) or to stop (del)")
    args = parser.parse_args()

    # Load the AWS Configurations
    config_file = open("config.json",'r')
    config = json.load(config_file)

    # Check which service to start
    # SQS Start
    if args.service == "sqs":
        #Connect to SQS API and Create a Queue
        Queue_conn = boto.sqs.connect_to_region(
            "us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])

        sqs_q = Queue_conn.create_queue('TasksQueue')
        print("SQS Task Queue Created ...")
        sqs_response_q = Queue_conn.create_queue('ResponseQueue')
        print("SQS Response Queue Created ...")

    # DynamoDB Start
    if args.service == "dydb":
        # Connect to DynamoDB and Create a table
        DB_conn = boto.dynamodb.connect_to_region(
            "us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])

        dynamo_table_schema = DB_conn.create_schema(
            hash_key_name = 'TaskID',
            hash_key_proto_value = int
        )

        dynamo_table = DB_conn.create_table(
            name = 'TasksData',
            schema = dynamo_table_schema,
            read_units = 20,
            write_units = 50
        )
        print("DynamoDB Table Created ...")

    # t2.micro start
    if args.service == "t2m":
        no_inst = input("How many Instances to Launch: ")
        for i in range(int(no_inst)):
            print("Launching t2.micro Instance "+str(i+1)+" please wait ...")
            Inst_conn = boto.ec2.connect_to_region(
                "us-east-1",
                aws_access_key_id = config["AWSAccessKeyId"],
                aws_secret_access_key = config["AWSSecretKey"])

            Inst_conn.run_instances('ami-3249ae5f',key_name='CloudSorting',instance_type='t2.micro',security_groups=['launch-wizard-2'])
        print(str(no_inst)+" Instance(s) Launched ...")

    # S3 start
    if args.service == "s3":
        S3_conn = boto.s3.connect_to_region("us-east-1",
                aws_access_key_id = config["AWSAccessKeyId"],
                aws_secret_access_key = config["AWSSecretKey"])
        S3_conn.create_bucket('animoto_video_storage_shalin')
        print("Bucket Created ...")

    # Purge Queues and Delete DynamoDB
    if args.service == "del":
        Queue_conn = boto.sqs.connect_to_region(
            "us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])

        DB_conn = boto.dynamodb.connect_to_region(
            "us-east-1",
            aws_access_key_id = config["AWSAccessKeyId"],
            aws_secret_access_key = config["AWSSecretKey"])

        Queue_conn.get_queue('TasksQueue').purge()
        Queue_conn.get_queue('ResponseQueue').purge()

        DB_conn.get_table('TasksData').delete()

        print("SQS Queues are Purged ...")
        print("Dynamo DB table deleted ...")