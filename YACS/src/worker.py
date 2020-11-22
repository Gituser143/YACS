#!/usr/bin/python3

import sys
import time
import socket
import json
import threading

if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "PORT WORKER_ID")
    exit(1)

port = int(sys.argv[1])
id = sys.argv[2]
server_ip = "localhost"

time.sleep(1)
print("Spawned worker {id} on port {p}".format(p=port, id=id))

execution_pool = []

def master_listener():

    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((server_ip, port))

    master.listen()
    while True:
        sock, address = master.accept()
        print("Got connection from", address)
        message = bytes()

        while True:
            data = sock.recv(1)
            if not data:
                break
            message += data

        message = message.decode("utf-8")

        task = json.loads(message)

        execution_pool.append(task)

def send_updates_to_master(task):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, 5001))
        message = json.dumps(task)
        s.send(message.encode())

def worker():

    while True:
        # infinite loop that waits for a task to be added to the execution pool
        while len(execution_pool) == 0:
            continue
        
        remaining_duration = execution_pool[0]["duration"]

        while remaining_duration:
            remaining_duration -= 1
            if remaining_duration == 0:
                task = execution_pool.pop(0)
                task["remaining_duration"] = 0
                task["worker_id"] = id
                send_updates_to_master(task)


master_listner_thread = threading.Thread(target=master_listener)
worker_thread = threading.Thread(target=worker)

master_listner_thread.start()
worker_thread.start()

master_listner_thread.join()
worker_thread.join()
