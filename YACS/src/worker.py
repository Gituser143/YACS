#!/usr/bin/python3

import sys
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

task_listener.listen(1)
while True:
    sock, address = task_listener.accept()

    message = bytes()
    while True:
        data = sock.recv(1024)
        if not data:
            break
        message += data
    message = message.decode()
    print("Got", message, "in worker", id)

    # ===================================
    # TODO: Add task to pool and process
    # ===================================
