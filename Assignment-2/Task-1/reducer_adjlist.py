#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}

for line in sys.stdin:

    line = line.strip()
    if line != '':
        if line[0] != '#':
            print(line)
            try:
                key, value = line.split("\t")
                if key not in dict:
                    dict[key] = []
                    dict[key].append(int(value))
                elif key in dict:
                    dict[key].append(int(value))

            except:
                continue

for key, value in dict.items():
    print(key, value)
