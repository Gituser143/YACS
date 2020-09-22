#!/usr/bin/python3
import sys
sum = 0
count = 0

try:
    file = str(sys.argv[1])
except:
    file = 'v1'
    print("Default input: v1")
with open(file, "r") as file:
    lines = file.readlines()
    for line in lines:
        count += 1
        sum += float(line.strip().split(",")[1])
    print(sum <= count)
    print(count, sum)
