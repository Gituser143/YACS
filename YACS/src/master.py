import sys
import os
import json
import socket
import threading
import random
import time

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def printMessage(type, message):
    if type == "LOG":
        print(bcolors.OKGREEN + message + bcolors.ENDC)

    elif type == "ERROR":
        print(bcolors.FAIL + bcolors.BOLD + message + bcolors.ENDC + bcolors.ENDC)

    else:
        print(bcolors.OKCYAN + bcolors.BOLD + message + bcolors.ENDC + bcolors.ENDC)


# Verifying command line arguments
if len(sys.argv) < 3:
    printMessage("ERROR", "ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "CONFIG.JSON SCHEDULING_ALGO")
    exit(1)

# Setting command line args
conf_path = "YACS/src/config.json"
conf_path = sys.argv[1]
sched_algo = sys.argv[2]
sched_algo = sched_algo.upper()




# Metatdata

'''
Stats holds information on each worker
Format:
  worker_id: [total_slots, slots_available, [ip, port]]
'''
stats = dict()

'''
Holds list of map and dependent reduce tasks for a given job
Format:
  jobId: {map: {task_id: map task}, reduce: {task_id: reduce task}}
'''
task_dependencies = dict()

'''
Holds job IDs in queue for scheduling
'''
job_queue = []

# variables used for Round Robin

'''
Used for round robin
Holds the worker IDs in a queue
'''
worker_queue = []

'''
Used for round robin
Holds the previous index of the worker used
'''
worker_prev_idx = -1

# Variables for Least-Loaded scheduling

'''
Holds current free slots for workers
Format:
  num_free_slots: [workers with said free slots]
'''
loads = dict()

# Mutexes to be used when modifying data
task_mutex = threading.Lock()
stats_mutex = threading.Lock()
queue_mutex = threading.Lock()


def init_meta(stats, sched_algo):
    '''
    Function to Initialise metatda. Takes in the
    scheduling algorithm chosen and a stats dictionary.
    '''
    # Least loaded by default
    if sched_algo == "RANDOM":
        pass
    elif sched_algo == "RR":
        stats_mutex.acquire()
        for worker_id in sorted(stats):
            worker_queue.append(worker_id)
        stats_mutex.release()
    else:
        # Init free slots in loads map
        for worker in stats:
            free_slots, _, _ = stats[worker]
            if free_slots in loads:
                loads[free_slots].append(worker)
            else:
                loads[free_slots] = [worker]


def send_job(worker_id, job_id, task, task_type):
    '''
    Given a worker and a task, the function sends
    the task to the worker through a socket.
    '''

    # Get socket details of specified worker
    worker_ip, worker_port = stats[worker_id][2]

    # Create message to send
    message = {"job_id": job_id, "task_type": task_type, "task": task, "worker_id": worker_id}

    printMessage("Task", "Scheduled task: " + message["task"]["task_id"] + " on worker:" + str(message["worker_id"]))
    # Open socket connection
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((worker_ip, worker_port))
        message = json.dumps(message)

        # Send Task
        s.send(message.encode())


def random_sched(job_id, task_type):
    '''
    The function schedules all tasks of task_type (map/reduce)
    from a given job with id = job_id. It is scheduled according
    to the random algorithm (scheduled randomly into a worker
    with free slots).
    '''

    # Get tasks
    task_mutex.acquire()
    tasks = task_dependencies[job_id][task_type].copy()
    task_mutex.release()

    for task in tasks:
        stats_mutex.acquire()
        stats_copy = stats.copy()
        stats_mutex.release()

        # Select worker
        chosen_worker = random.choice(list(stats_copy))

        # Ensure there exists an empty slot
        has_empty_slots.acquire()

        # Get a random worker with at least 1 empty slot
        while stats_copy[chosen_worker][1] == 0:
            del stats_copy[chosen_worker]
            chosen_worker = random.choice(list(stats_copy))

        # Update slot details in metadata
        stats_mutex.acquire()
        stats[chosen_worker][1] -= 1
        stats_mutex.release()

        send_job(chosen_worker, job_id, tasks[task], task_type)


def round_robin_sched(job_id, task_type):
    '''
    This fucntion schedules all tasks of type task_type
    from job with id = job_id. The tasks are scheduled
    using round robin algorithm (Cyclically schedule tasks
    across workers with empty slots).
    '''

    global worker_prev_idx

    # Get the tasks
    task_mutex.acquire()
    tasks = task_dependencies[job_id][task_type].copy()
    task_mutex.release()

    for task in tasks:
        # Ensure cluster has empty slots
        has_empty_slots.acquire()

        # Get a copy of stats
        stats_mutex.acquire()
        stats_copy = stats.copy()
        stats_mutex.release()

        # Get current worker index
        queue_mutex.acquire()
        curr_idx = (worker_prev_idx+1) % len(worker_queue)
        queue_mutex.release()

        # Choose a worker
        chosen_worker = worker_queue[curr_idx]

        # Finding a worker with a free slot
        while stats_copy[chosen_worker][1] == 0:
            curr_idx = (curr_idx + 1) % len(worker_queue)
            chosen_worker = worker_queue[curr_idx]

        # Decrement slot for chosen worker
        stats_mutex.acquire()
        stats[chosen_worker][1] -= 1
        stats_mutex.release()

        # Keep track of the previously used worker index
        queue_mutex.acquire()
        worker_prev_idx = curr_idx
        queue_mutex.release()

        # Send job to worker
        send_job(chosen_worker, job_id, tasks[task], task_type)


def least_loaded_sched(job_id, task_type):
    '''
    This fucntion schedules all tasks of type task_type
    from job with id = job_id. The tasks are scheduled
    using least loaded algorithm (Assign task to workers
    with least amount of load, i.e, more number of free slots).
    '''

    # Get tasks to run
    task_mutex.acquire()
    tasks = task_dependencies[job_id][task_type].copy()
    task_mutex.release()

    for task_id in tasks:
        task = tasks[task_id]

        # Ensure empty slots are available
        has_empty_slots.acquire()

        # Critical section to update metatdata
        stats_mutex.acquire()

        # Get least load
        least_load = max(loads)

        # Get least loaded worker
        worker_id = loads[least_load].pop(0)
        if len(loads[least_load]) == 0:
            del loads[least_load]

        # Update free slots metatdata
        if least_load-1 in loads:
            loads[least_load-1].append(worker_id)
        else:
            loads[least_load-1] = [worker_id]

        stats_mutex.release()

        # Send task to worker
        send_job(worker_id, job_id, task, task_type)


def schedule_job(job_id, sched_algo="LL", task_type="map"):
    '''
    Given a job ID and a scheduling algorithm,
    the function schedules all tasks of specified
    type in the given job on workers according to
    the scheduing algorithm specified. The scheduling
    algorithm defaults to least loaded when not specified.
    '''

    if sched_algo == "RANDOM":
        random_sched(job_id, task_type)
    elif sched_algo == "RR":
        round_robin_sched(job_id, task_type)
    else:
        least_loaded_sched(job_id, task_type)


def client_listener(n):
    '''
    Function which listens to job submissions
    from the client. Extracts information and
    proceeds to update metadata and schedule job.
    '''

    # Initialise socket
    server_ip = "localhost"
    client_port = 5000

    # Bind socket for client interactions
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.bind((server_ip, client_port))

    # Listen for incoming requests from clients
    client.listen(2)
    while True:
        sock, address = client.accept()
        message = bytes()
        while True:
            data = sock.recv(1024)
            if not data:
                break
            message += data

        message = message.decode()

        # Load into json format
        job_data = json.loads(message)
        job_id = job_data["job_id"]
        map_tasks = job_data["map_tasks"]
        reduce_tasks = job_data["reduce_tasks"]

        map_dict = {task["task_id"]: task for task in map_tasks}
        reduce_dict = {task["task_id"]: task for task in reduce_tasks}

        # Add job to list of task dependencies
        task_mutex.acquire()
        task_dependencies[job_id] = {"map": map_dict, "reduce": reduce_dict}
        task_mutex.release()

        # Add job to execution queue
        queue_mutex.acquire()
        job_queue.append(job_id)
        has_jobs.release()
        queue_mutex.release()


def worker_listener(n):
    '''
    Function which listens to task completions
    from the client. Extracts information and
    proceeds to update metadata and schedule
    dependent tasks if any.
    '''

    def worker_connection(sock):
        '''
        Function used to handle one worker connection.
        Called with threads to support threaded server.
        '''
        message = bytes()
        while True:
            data = sock.recv(1024)
            if not data:
                break
            message += data

        message = message.decode()
        response = json.loads(message)

        # Extract data from response
        job_id = response["job_id"]
        task_id = response["task"]["task_id"]
        task_type = response["task_type"]
        worker_id = response["worker_id"]

        task_mutex.acquire()

        printMessage("Task", "Completed task: " + task_dependencies[job_id][task_type][task_id]["task_id"] + " on worker: " + str(worker_id))
        # Remove job from dependencies
        del task_dependencies[job_id][task_type][task_id]

        task_mutex.release()

        # Update metadata
        if sched_algo == "RR":
            stats_mutex.acquire()
            stats[worker_id][1] += 1
            stats_mutex.release()

        elif sched_algo == "RANDOM":
            # Update free slots
            stats_mutex.acquire()
            stats[worker_id][1] += 1
            stats_mutex.release()

        else:
            stats_mutex.acquire()

            # Update free slots for worker
            for free_slots in loads:
                if worker_id in loads[free_slots]:
                    loads[free_slots].remove(worker_id)
                    break

            if free_slots+1 in loads:
                loads[free_slots+1].append(worker_id)
            else:
                loads[free_slots+1] = [worker_id]

            stats_mutex.release()

        # Increment empty slots
        has_empty_slots.release()

        # If no more map tasks exist, schedule reduce tasks
        if task_type == "map":
            task_mutex.acquire()
            remaining_map_tasks = len(task_dependencies[job_id][task_type])
            task_mutex.release()

            if remaining_map_tasks == 0:
                schedule_job(job_id, sched_algo, "reduce")
        else:
            task_mutex.acquire()
            remaining_reduce_tasks = len(task_dependencies[job_id][task_type])
            task_mutex.release()

            if remaining_reduce_tasks == 0:
                printMessage("LOG", "Completed job: " + job_id)
                task_mutex.acquire()
                del task_dependencies[job_id]
                task_mutex.release()

    # Initialise socket
    server_ip = "localhost"
    worker_port = 5001

    # Bind socket for worker interactions
    worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    worker.bind((server_ip, worker_port))

    # Initialise list of threads
    threads = []

    # Listen for incoming requests from workers
    while True:
        worker.listen(n)
        sock, address = worker.accept()

        new_thread = threading.Thread(target=worker_connection, args=(sock,))

        new_thread.start()

        threads.append(new_thread)

    for thread in threads:
        thread.join()


def job_scheduler():
    '''
    Function which picks jobs from the job queue
    and schedules them according to preset
    scheduling algorithm.
    '''
    while True:
        # Check if jobs are in queue
        has_jobs.acquire()

        # Get job to schedule
        queue_mutex.acquire()
        job_id = job_queue.pop(0)
        queue_mutex.release()

        printMessage("LOG", "Started job: " + job_id)

        # Schedule job
        schedule_job(job_id, sched_algo)


# Load JSON config
config_file = open(conf_path, "r")
json_data = json.load(config_file)

# Get information of workers
workers = json_data["workers"]

total_slots = 0

# Parse workers
for worker in workers:
    worker_id = worker["worker_id"]
    slots = worker["slots"]
    port = worker["port"]

    # Insert meta
    stats[worker_id] = [slots, slots, ["localhost", port]]
    total_slots += slots

    # Command line args meant for worker.py
    args = ["worker.py", str(port), str(worker_id)]

    # Create worker processes
    pid = os.fork()
    if pid == -1:
        printMessage("ERROR", "Failed to spawn worker {id}".format(id=worker_id))
    elif pid == 0:
        os.execv("worker.py", args)

# Initialise metatdata
has_empty_slots = threading.Semaphore(total_slots)
has_jobs = threading.Semaphore(0)
init_meta(stats, sched_algo)

n = len(workers)
print("Spawned {n} workers".format(n=n))


# Create threads to listen for clients and workers
client_thread = threading.Thread(target=client_listener, args=(n,))
worker_thread = threading.Thread(target=worker_listener, args=(n,))
scheduler_thread = threading.Thread(target=job_scheduler)

# Start threads
client_thread.start()
worker_thread.start()
scheduler_thread.start()

# Wait for threads to finish
client_thread.join()
worker_thread.join()
scheduler_thread.join()
