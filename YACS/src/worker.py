#!/usr/bin/python3

import sys
import json
import time
import socket
import threading
import queue


if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "PORT WORKER_ID")
    exit(1)

ip = "localhost"
port = int(sys.argv[1])
id = sys.argv[2]

print("Spawned worker {id} on port {p}".format(p=port, id=id))

server_ip = "localhost"

execution_pool = []

# execution_mutex = threading.Lock()
# has_tasks = threading.Semaphore(0)

# ===================================================
# Create separate threads to listen and process tasks
# ===================================================


def decrement_duration(task):

    print(task)
    while task["task"]["duration"]:
        time.sleep(1)
        task["task"]["duration"] -= 1
    send_updates_to_master(task)


def master_listener():
    '''
    Listens to messages coming from master.
    Master sends newly scheduled tasks.
    '''

    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((server_ip, port))
    master.listen()
    while True:
        sock, address = master.accept()
        message = bytes()

        while True:
            data = sock.recv(1)
            if not data:
                break
            message += data

        message = message.decode()

        # Newly added tasks are added to the execution pool
        task = json.loads(message)
        execution_thread = threading.Thread(target=decrement_duration, args=(task,))
        execution_thread.start()
        execution_pool.append(execution_thread)


def send_updates_to_master(task):
    '''
    Sends updates aboute completed tasks back to the master.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, 5001))
        message = json.dumps(task)
        s.send(message.encode())


def worker():
    '''
    Simulates execution of tasks in the execution pool
    '''
    while True:
        for exec_thread in execution_pool:
            exec_thread.join()


master_listner_thread = threading.Thread(target=master_listener)
worker_thread = threading.Thread(target=worker)

master_listner_thread.start()
worker_thread.start()

master_listner_thread.join()
worker_thread.join()
