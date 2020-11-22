import sys
import os
import json
import socket
import threading
import random

# Verifying command line arguments
if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "CONFIG.JSON SCHEDULING_ALGO")
    exit(1)

# Setting command line args
conf_path = "YACS/src/config.json"
conf_path = sys.argv[1]
sched_algo = sys.argv[2]

# Load JSON config
config_file = open(conf_path, "r")
json_data = json.load(config_file)

# Get information of workers
workers = json_data["workers"]

# Stats holds information on each worker
# format = worker_id: [total_slots, slots_available, [ip, port]]
stats = dict()

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
        print("Failed to spawn worker {id}".format(id=worker_id))
    elif pid == 0:
        os.execv("worker.py", args)
print(stats)

n = len(workers)
print("Spawned {n} workers".format(n=n))

def send_request(task, address):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((address[0], address[1]))
        message = json.dumps(task)
        s.send(message.encode())

def scheduler(job):

    while job["status"] != 0:
        if job["status"] == 2:
            map_tasks = job["map_tasks"]
            if sched_algo == 'RANDOM':
                random_sched(map_tasks, job["job_id"], "M")
            elif sched_algo == 'RR':
                # round_robin_sched(map_tasks)
                print("RR")
            else:
                # least_loaded_sched(map_tasks)
                print("LL")
            
            # job["status"] = 1
        if job["status"] == 1:
            reduce_tasks = job["reduce_tasks"]
            if sched_algo == 'RANDOM':
                random_sched(reduce_tasks, job["job_id"], "R")
            elif sched_algo == 'RR':
                # round_robin_sched(reduce_tasks)
                print("RR")
            else:
                # least_loaded_sched(reduce_tasks)
                print("LL")

def random_sched(job_id, task_type):
    tasks = jobs["job_id"]
    for task in tasks:
        chosen_worker = random.choice(list(stats))

        # put semaphore here instead of while loop
        while stats[chosen_worker][1] == 0:
            chosen_worker = random.choice(list(stats))

        task["type"] = task_type
        task["job_id"] = job_id
        stats[chosen_worker][1] -= 1
        send_request(task, stats[chosen_worker][2])

def round_robin_sched(job_id, task_type):

    worker_ids = list(stats)


jobs = dict()

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
        # print(message)

        # ======================================
        # Process job details and schdeule
        # ======================================

        message = message.decode("utf-8")
        json_job = json.loads(message)
        # status -> 2 => incomplete map and reduce, 
        # status -> 1 => complete map, incomplete red, 
        # status -> 0 => complete map and reduce
        # completed -> [0, 0] => 0 completed map tasks, 0 completed reduce tasks
        jobs[json_job["job_id"]] = {"job_id": json_job["job_id"], \
                                    "map_tasks": json_job["map_tasks"], \
                                    "reduce_tasks": json_job["reduce_tasks"], \
                                    "status": 2, \
                                    "completed": [0, 0]\
                                    }
        print(jobs)

        scheduler(jobs[json_job["job_id"]])

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
        print("Got connection from ", address)
        message = bytes()
        while True:
            data = sock.recv(1024)
            if not data:
                break
            message += data
        # =====================================
        # TODO: Update metadata on completions
        # =====================================

        message = message.decode("utf-8")
        worker_response = json.loads(message)

        stats[int(worker_response["worker_id"])][1] += 1

        if worker_response["type"] == 'M':
            jobs[worker_response["job_id"]]["completed"][0] += 1
            if jobs[worker_response["job_id"]]["completed"][0] == len(jobs[worker_response["job_id"]]["map_tasks"]):
                jobs[worker_response["job_id"]]["status"] = 1
        elif worker_response["type"] == 'R':
            jobs[worker_response["job_id"]]["completed"][1] += 1
            if jobs[worker_response["job_id"]]["completed"][1] == len(jobs[worker_response["job_id"]]["reduce_tasks"]):
                jobs[worker_response["job_id"]]["status"] = 0


# Create threads to listen for clients and workers
client_thread = threading.Thread(target=client_listener, args=(n,))
worker_thread = threading.Thread(target=worker_listener, args=(n,))

# Start both threads
client_thread.start()
worker_thread.start()

# Wait for threads to finish
client_thread.join()
worker_thread.join()
