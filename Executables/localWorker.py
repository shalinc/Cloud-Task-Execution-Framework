"""
    This code is for Local Worker Execution
    Here the Local Worker Executes the Task allocated to it
"""

import queue
import time
from threading import Thread

# LocalWorker class
class localWorker(Thread):

    # Ctor
    def __init__(self, tasks_queue, response_queue):
        Thread.__init__(self)
        self.tasks_queue = tasks_queue
        self.response_queue = response_queue

    # Overriding the Thread Run method with self-Run Method
    def run(self):
        # Execute the Tasks
        # If Success add 0 to response queue else add 1 for failure
        try:
            # Extract while Queue is not Empty
            while not self.tasks_queue.empty():
                # Extract SLEEP task time from QUEUE
                sleepTime = self.tasks_queue.get()
                # Conversion to MilliSeconds
                time.sleep(int(sleepTime.split(' ')[1])/1000.0)
                self.response_queue.put(0)
        except IOError as e:
            self.response_queue.put(1)
            print("ERROR Occured: ", e)

# main
if __name__ == '__main__':
    pass
