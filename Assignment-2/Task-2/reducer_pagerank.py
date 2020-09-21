#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split()
        if key not in dict:
            dict[key] = int(value)
        elif key in dict:
            dict[key] += int(value)
    except:
        continue

for key in dict:
    print(key, 0.15 + (0.85*dict[key]), sep=",")
