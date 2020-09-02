#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}
for line in sys.stdin:
    # print(line)
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

for i in dict:
    print(dict[i])
    
