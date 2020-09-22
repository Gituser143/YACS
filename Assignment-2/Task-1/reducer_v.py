#!/usr/bin/python3
import sys

dict = {}

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split()
        if key not in dict:
            dict[key] = 1

        if value not in dict:
            dict[value] = 1
    except:
        continue

for key in dict:
    print(key, 1, sep=",")
