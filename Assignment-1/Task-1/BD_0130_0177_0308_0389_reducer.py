#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}
dict["1"] = 0
dict["2"] = 0
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

print(dict["1"])
print(dict["2"])
