#!/usr/bin/python3

import sys
import json
import time
import socket

if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "PORT WORKER_ID")
    exit(1)

ip = "localhost"
port = sys.argv[1]
id = sys.argv[2]

print("Spawned worker {id} on port {p}".format(p=port, id=id))

# ===================================================
# Create separate threads to listen and process tasks
# ===================================================

task_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
task_listener.bind((ip, int(port)))

task_listener.listen()
while True:
    sock, address = task_listener.accept()

    message = bytes()
    while True:
        data = sock.recv(1024)
        if not data:
            break
        message += data
    message = message.decode()

    message = json.loads(message)
    print("Got", message["task"]["task_id"], "in worker", id)

    response = dict()
    response["job_id"] = message["job_id"]
    response["task_id"] = message["task"]["task_id"]
    response["task_type"] = message["task_type"]
    response["worker_id"] = id

    # Open socket connection
    master_ip = "localhost"
    master_port = 5001
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((master_ip, master_port))
        response = json.dumps(response)

        # Send Task
        s.send(response.encode())

    # ===================================
    # TODO: Add task to pool and process
    # ===================================
