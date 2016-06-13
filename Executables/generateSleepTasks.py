"""
    Code to generate the Sleep Tasks and Animoto Jobs
"""

# generate tasks for Throughput
def generate_tasks(sleep_time, count):

    try:
        file = open("tasks_sleep"+sleep_time+"_"+str(count)+".txt",'w+')
        for i in range(count):
            file.write("sleep "+sleep_time+"\n")
        file.close()

    except IOError as e:
        print("Error Occurred: ",e)

# generate tasks for Efficiency
def generate_tasks_efficiency():
    workers = [1,2,4,8,16]
    sleep_time = [10, 1000, 10*1000]
    no_tasks = [1000, 100, 10]
    for i in workers:
        i_t = 0
        for st in sleep_time:
            file = open("worker_"+str(i)+"_sleep_"+str(st)+".txt",'w+')
            for nt in range(i*no_tasks[i_t]):
                file.write("sleep "+str(st)+"\n")
            file.close()
            i_t+=1

# generate tasks for Animoto
def generate_animoto_jobs():
    file = open("Animoto_Jobs.txt",'w+')
    for i in range(160):
        file.write("Job_"+str(i)+".txt"+"\n")
    file.close()


if __name__ == "__main__":

    # Sleep time to set
    sleep_time = "0"

    # Count of Tasks
    count = 10000

    # Generate Tasks for Throughput
    generate_tasks(sleep_time, count)

    # Generate for Animoto Jobs
    #generate_animoto_jobs()

    # Generate Tasks for Efficiency
    #generate_tasks_efficiency()