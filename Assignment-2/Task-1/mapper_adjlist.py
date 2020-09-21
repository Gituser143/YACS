#!/usr/bin/python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        if line != '':
            if line[0] != '#':
                print(line)
    except:
        continue
