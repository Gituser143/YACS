import sys
import os
import json
import socket
import threading

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
        print("Faile to spawn worker {id}".format(id=worker_id))
    elif pid == 0:
        os.execv("worker.py", args)

n = len(workers)
print("Spawned {n} workers".format(n=n))


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
        print(message)

    # ======================================
    # TODO: Process job details and schdeule
    # ======================================


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
        # =====================================
        # TODO: Update metadata on completions
        # =====================================


# Create threads to listen for clients and workers
client_thread = threading.Thread(target=client_listener, args=(n,))
worker_thread = threading.Thread(target=worker_listener, args=(n,))

# Start both threads
client_thread.start()
worker_thread.start()

# Wait for threads to finish
client_thread.join()
worker_thread.join()
