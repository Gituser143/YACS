#!/usr/bin/python3

import sys
import json
import time
import socket
import threading
import datetime
import os

if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "PORT WORKER_ID")
    exit(1)

server_ip = "localhost"
port = int(sys.argv[1])
id = sys.argv[2]

# Logging
log_file_path = "worker_" + id + ".log"
if os.path.exists(log_file_path):
    os.remove(log_file_path)


def log_message(message):
    '''
    Function which logs given message by adding
    a timestamp and writing to specified log file.
    '''
    current_time = datetime.datetime.now()
    log = "[" + str(current_time) + "]"
    log += " " + message
    log_file = open(log_file_path, "a+")

    log_file.write(log + "\n")
    log_file.close()


print("Spawned worker {id} on port {p}".format(p=port, id=id))


execution_pool = []


def decrement_duration(task):
    '''
    Run each task. Sleep in intervals of 1 second
    for given task duration. On finishing task,
    update master.
    '''
    while task["task"]["duration"]:
        time.sleep(1)
        task["task"]["duration"] -= 1
    log_message("Completed task: " + task["task"]["task_id"])
    send_updates_to_master(task)


def master_listener():
    '''
    Listens to messages coming from master.
    Master sends newly scheduled tasks.
    '''

    # Listen for tasks from master
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((server_ip, port))
    master.listen()
    while True:
        sock, address = master.accept()
        message = bytes()

        while True:
            data = sock.recv(1024)
            if not data:
                break
            message += data

        message = message.decode()

        task = json.loads(message)

        log_message("Task arrived: " + task["task"]["task_id"])

        # Start each task on an individual threrad.
        execution_thread = threading.Thread(target=decrement_duration, args=(task,))
        execution_thread.start()

        # Add thread to execution_pool
        execution_pool.append(execution_thread)


def send_updates_to_master(task):
    '''
    Sends updates about completed tasks back to the master.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        message = dict()
        message["job_id"] = task["job_id"]
        message["task_id"] = task["task"]["task_id"]
        message["task_type"] = task["task_type"]
        message["worker_id"] = task["worker_id"]
        s.connect((server_ip, 5001))
        message = json.dumps(message)
        s.send(message.encode())


def worker():
    '''
    Simulates execution of tasks in the execution pool
    '''
    while True:
        for exec_thread in execution_pool:
            exec_thread.join()

        execution_pool.clear()


master_listner_thread = threading.Thread(target=master_listener)
worker_thread = threading.Thread(target=worker)

master_listner_thread.start()
worker_thread.start()

master_listner_thread.join()
worker_thread.join()
