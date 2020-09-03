#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}
for line in sys.stdin:

    line = line.strip()
    try:
        word, count = line.split()
        if word not in dict:
            dict[word] = 1
        elif word in dict:
            dict[word] += 1

    except:
        continue

for key in sorted(dict.keys()):
    print(key, dict[key], sep=",")
