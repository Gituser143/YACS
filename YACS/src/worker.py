#!/usr/bin/python3

import sys
import json
import time
import socket
import threading

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

execution_mutex = threading.Lock()
has_tasks = threading.Semaphore(0)

# ===================================================
# Create separate threads to listen and process tasks
# ===================================================


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
        execution_mutex.acquire()
        execution_pool.append(task)
        has_tasks.release()
        execution_mutex.release()


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

    # Keeps on running waiting for any new tasks to be added to the execution pool.
    while True:

        # Sleep 1 second irrespective of any tasks being in the execution pool.
        time.sleep(1)
        execution_mutex.acquire()

        # If tasks exist, decrement duration for each.
        if len(execution_pool) != 0:
            to_pop = []
            for task in execution_pool:
                if task["task"]["duration"] == 1:
                    to_pop.append(task)
                else:
                    task["task"]["duration"] -= 1

                # If duration is 0 (task completed), pop and send message to master.
                # if task["task"]["duration"] == 0:
                #     # time.sleep(1)
            for task in to_pop:
                execution_pool.remove(task)
                send_updates_to_master(task)
        execution_mutex.release()


master_listner_thread = threading.Thread(target=master_listener)
worker_thread = threading.Thread(target=worker)

master_listner_thread.start()
worker_thread.start()

master_listner_thread.join()
worker_thread.join()
