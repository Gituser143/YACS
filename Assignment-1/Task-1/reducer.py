#!/usr/bin/python3
"""reducer.py"""

import sys

count1 = 0
count2 = 0

for line in sys.stdin:
    line = line.strip()

    word, count = line.split()
    if(word == "1"):
        count1+=1
    elif(word == "2"):
        count2+=1

print(count1)
print(count2)
