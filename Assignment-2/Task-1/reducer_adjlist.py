#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}

for line in sys.stdin:
    line = line.strip()
    if line != '':
        if line[0] != '#':
            try:
                key, value = line.split("\t")
                if key not in dict:
                    dict[key] = int(value)
                elif key in dict:
                    dict[key] += int(value)
            except:
                continue

for key in dict:
    print(key, dict[key], sep="\t")
