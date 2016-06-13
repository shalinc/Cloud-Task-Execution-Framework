"""
    This is a Client.py File
    For Executing Local Task Execution Framework
    The Code requires command line arguments to be passed
    Help for the same is found by typing client.py -h
"""

import argparse
import queue
from localWorker import localWorker
import time
import os

# Class Client
class client(object):

    # Class Variables
    task_queue = queue.Queue()
    response_queue = queue.Queue()

    # Ctor
    def __init__(self, q_name, workload_file):
        self.q_name = q_name
        self.workload_file = workload_file

    # Methods

    # Fetch tasks from WorkLoad File
    def fetch_tasks(self):
        try:
            file_tasks = open(self.workload_file,'r').readlines()
            #print(file_tasks)
        except IOError as e:
            print("ERROR OCCURED: ",e)
        return file_tasks

    # Put tasks onto in-Memory Queue
    def send_tasks_queue(self, tasks):
        for task in tasks:
            self.task_queue.put(task.strip())
        return self.task_queue

    # Create ThreadPool
    def createThreadPool(self,N):
        local_workers = [localWorker(self.task_queue, self.response_queue) for th in range(N)]
        return local_workers


# main
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-s',metavar="QUEUE NAME",type=str,help="the Name of the QUEUE (LOCAL/SQS_QUEUE_NAME)", required=True)
    parser.add_argument('-t',metavar="NUMBER OF THREADS",type=str,help="Number of Threads in the Pool", required=True)
    parser.add_argument('-w',metavar="WORKLOAD FILE",type=str,help="Path of the Workload File", required=True)
    args = parser.parse_args()

    q_name = args.s
    N = int(args.t)
    workload_file = os.getcwd()+"/Workloads/"+args.w
    response_file = os.getcwd()+"/ResponseLogs/"+args.w

    client = client(q_name, workload_file)

    # Fetch the tasks from the Workload File
    tasks = client.fetch_tasks()
    print("Fetched the Tasks from Worload File ...")

    #Put the tasks onto a QUEUE
    start = time.time()
    task_queue = client.send_tasks_queue(tasks)
    print("Tasks are in in-memory Queue")

    #Make a Pool of Threads and Start Local Workers
    local_workers = client.createThreadPool(N)

    #Run tasks
    for worker in local_workers:
        worker.start()

    for worker in local_workers:
        worker.join()

    end = time.time()
    print("All Tasks Executed Successfully ...")
    print("Time taken for "+str(N)+" local workers:",str(end-start)+" sec")

    #writing Log File
    # Adding response to the output_file
    print("Writing Logs Please wait ...")
    file = open(response_file+"_Log.txt",'w+')

    #print(client.response_queue.qsize())

    while not client.response_queue.empty():
       file.write(str(client.response_queue.get())+"\n")
    file.close()

    print("Logs written Successfully in",response_file+"_Log.txt")