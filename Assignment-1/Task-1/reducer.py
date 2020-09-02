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
        print("Error")
        exit(1)

for key in dict:
    print(dict[key])

# Execution = echo "airplane" | ./mapper.py | ./reducer.py
# Execution = echo "aircraft carrier" | ./mapper.py | ./reducer.py
