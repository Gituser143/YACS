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

# Load JSON config
config_file = open(conf_path, "r")
json_data = json.load(config_file)

# Get information of workers
workers = json_data["workers"]

# Stats holds information on each worker
# Format:
#   worker_id: [total_slots, slots_available, [ip, port]]
stats = dict()

# Holds list of map and dependent reduce tasks for a given job
# Format:
#   jobId: [[map tasks][reduce tasks]]
task_dependencies = dict()

# Mutex to be used when modifying data
task_mutex = threading.Lock()
stats_mutex = threading.Lock()


# Variables for Least-Loaded

# Holds current free slots for workers
# Format:
#   num_free_slots: [workers with said free slots]
loads = dict()


def init_meta(stats, sched_algo):
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


def schedule_job(job_ID, sched_algo):
    if sched_algo == "RANDOM":
        pass
    elif sched_algo == "RR":
        pass
    else:

        # Get Job to run
        task_mutex.acquire()
        job = task_dependencies[job_ID]
        task_mutex.release()

        # Get least load
        stats_mutex.acquire()
        least_load = max(loads)
        stats_mutex.release()

        # =======================================
        # TODO: Replace busy wait with semaphore
        # =======================================
        while least_load == 0:
            time.sleep(1)
            stats_mutex.acquire()
            least_load = max(loads)
            stats_mutex.release()

        # Get least loaded worker
        stats_mutex.acquire()
        worker = loads[least_load][0]
        stats_mutex.release()

        # Assign slot of worker for task
        stats_mutex.acquire()
        # Remove worker from current loads and add to loads-1
        # Send task to worker
        # Repeat process for all map tasks
        stats_mutex.release()


def client_listener(n):
    # Initialise socket
    server_ip = "localhost"
    client_port = 5000

    print("Inside thread")

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
            data = sock.recv(1)
            if not data:
                break
            message += data

        message = message.decode()

        # Load into json format
        job_data = json.loads(message)
        job_id = job_data["job_id"]
        map_tasks = job_data["map_tasks"]
        reduce_tasks = job_data["reduce_tasks"]

        # Add job to list of task dependencies
        task_mutex.acquire()
        task_dependencies[job_id] = [map_tasks, reduce_tasks]
        task_mutex.release()

        print(job_id, map_tasks, reduce_tasks)

    # =========================================
    # TODO: Schdeule map tasks and update meta
    # =========================================


def worker_listener(n):
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
        print(message)

        # ========================================
        # TODO: Update task_dependencies and meta
        # ========================================

        # ===================================================
        # TODO: Schedule Reduce task if all map have finished
        # ===================================================


# Parse workers
for worker in workers:
    worker_id = worker["worker_id"]
    slots = worker["slots"]
    port = worker["port"]

    # Insert meta
    stats[worker_id] = [slots, slots, ["localhost", port]]

    # Command line args meant for worker.py
    args = ["worker.py", str(port), str(worker_id)]

    # Create worker processes
    pid = os.fork()
    if pid == -1:
        print("Faile to spawn worker {id}".format(id=worker_id))
    elif pid == 0:
        os.execv("worker.py", args)

# Initialise metatdata
init_meta(stats, sched_algo)

print(loads)

n = len(workers)
print("Spawned {n} workers".format(n=n))


# Create threads to listen for clients and workers
client_thread = threading.Thread(target=client_listener, args=(n,))
worker_thread = threading.Thread(target=worker_listener, args=(n,))

# Start both threads
client_thread.start()
worker_thread.start()

# Wait for threads to finish
client_thread.join()
worker_thread.join()
