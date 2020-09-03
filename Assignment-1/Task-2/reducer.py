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

finalList = []

for key in dict:
    str1 = (str(key)+","+str(dict[key]))
    finalList.append(str1)

finalList.sort()

for val in finalList:
    print(val)
