#!/usr/bin/python3
import sys

dict = {}

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split()
        print(key, 1, sep=",")
    except:
        continue
