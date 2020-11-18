import sys
import os
import json
import socket

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

# Initialise socket
serverIP = "localhost"
serverPort = 5000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((serverIP, serverPort))
s.listen(n)

while True:
    sock, address = s.accept()

    message = bytes()
    while True:
        data = sock.recv(1)
        if not data:
            break
        message += data
    print(message)
