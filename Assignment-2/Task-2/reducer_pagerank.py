#!/usr/bin/python3
import sys

dict = {}

for line in sys.stdin:
    try:
        line = line.strip()
        
        key, value = line.split()
        if key not in dict:
            dict[key] = float(value)
        elif key in dict:
            dict[key] += float(value)
    except:
        continue

for key in dict:
    print(key, 0.15 + (0.85*dict[key]), sep=",")
