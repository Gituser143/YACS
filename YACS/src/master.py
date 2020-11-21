import sys
import os
import json
import socket
import threading
import time

# message = b'{"job_id": "0", "map_tasks": [{"task_id": "0_M0", "duration": 4}, {"task_id": "0_M1", "duration": 1}], "reduce_tasks": [{"task_id": "0_R0", "duration": 3}, {"task_id": "0_R1", "duration": 3}]}'

# job_data = json.loads(message)

# print(job_data.keys())

# Verifying command line arguments
if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
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
        pass
    else:
        # Init free slots in loads map
        for worker in stats:
            free_slots, _, _ = stats[worker]
            if free_slots in loads:
                loads[free_slots].append(worker)
            else:
                loads[free_slots] = [worker]


def send_job(worker_id, job_id, task):
    '''
    Given a worker and a task, the function sends
    the task to the worker through a socket.
    '''

    # Get socket details of specified worker
    worker_ip, worker_port = stats[worker_id][2]

    # Create message to send
    message = {"job_id": job_id, "task": task}

    # Open socket connection
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((worker_ip, worker_port))
        message = json.dumps(str(message))

        # Send Task
        s.send(message.encode())


def schedule_job(job_id, sched_algo, task_type="map"):
    '''
    Given a job ID and a scheduling algorithm,
    the function schedules all map tasks in
    the given job on workers according to the
    scheduing algorithm.
    '''

    # Get Job to run
    task_mutex.acquire()
    job = task_dependencies[job_id]
    task_mutex.release()

    # Get tasks with no dependencies
    tasks = job[task_type]

    if sched_algo == "RANDOM":
        pass
    elif sched_algo == "RR":
        pass
    else:
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
            send_job(worker_id, job_id, task)


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
        print("Got connection from", address)
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

    # =====================================
    # TODO: Multithread the worker listener
    # =====================================

    # Initialise socket
    server_ip = "localhost"
    worker_port = 5001

    # Bind socket for worker interactions
    worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker.bind((server_ip, worker_port))

    # Listen for incoming requests from workers
    worker.listen(n)
    while True:
        sock, address = worker.accept()

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
        task_id = response["task_id"]
        task_type = response["task_type"]
        worker_id = response["worker_id"]

        task_mutex.acquire()

        # Remove job from dependencies
        del task_dependencies[job_id][task_type][task_id]

        task_mutex.release()

        # Update metadata
        if sched_algo == "RR":
            pass
        elif sched_algo == "RANDOM":
            pass
        else:
            stats_mutex.acquire()

            # Update free slots for worker
            for free_slots in loads:
                if worker_id in loads[free_slots]:
                    loads[free_slots].remove(worker_id)

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


def job_scheduler():
    while True:
        # Check if jobs are in queue
        has_jobs.acquire()

        # Get job to schedule
        queue_mutex.acquire()
        job_id = job_queue.pop(0)
        queue_mutex.release()

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
        print("Faile to spawn worker {id}".format(id=worker_id))
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
