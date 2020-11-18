#!/usr/bin/python3

import sys
import time

if len(sys.argv) < 3:
    print("ERROR: Not Enough Arguments")
    print("Usage:", sys.argv[0], "PORT WORKER_ID")
    exit(1)

port = sys.argv[1]
id = sys.argv[2]

time.sleep(1)
print("Spawned worker {id} on port {p}".format(p=port, id=id))
