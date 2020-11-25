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
        print("Got connection from", address)
        message = bytes()

        while True:
            data = sock.recv(1)
            if not data:
                break
            message += data

        message = message.decode("utf-8")

        # Newly added tasks are added to the execution pool
        task = json.loads(message)
        execution_mutex.acquire()
        execution_pool.append(task)
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

    # Keeps on running waiting for any new tasks to be added to the execution pool
    while True:
        # infinite loop that waits for a task to be added to the execution pool
        while len(execution_pool) == 0:
            continue
        
        # Decrements the remaining duration by 1 for all tasks that have not been completed but are in the execution pool
        # every 1 second
        execution_mutex.acquire()
        while len(execution_pool) != 0:
            time.sleep(1)
            for i in range(len(execution_pool)):
                if execution_pool[i]["task"]["duration"] <= 0:
                    completed_task = execution_pool.pop(i)
                    send_updates_to_master(completed_task)
                elif execution_pool[i]["task"]["duration"] > 0:
                    execution_pool[i]["task"]["duration"] -= 1
        execution_mutex.release()


master_listner_thread = threading.Thread(target=master_listener)
worker_thread = threading.Thread(target=worker)

master_listner_thread.start()
worker_thread.start()

master_listner_thread.join()
worker_thread.join()
