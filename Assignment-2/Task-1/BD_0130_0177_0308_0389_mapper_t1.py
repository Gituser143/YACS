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


# command to run locally:
# cat web-Google.txt | ./mapper_adjlist.py | sort -k1,1 | ./reducer_adjlist.py "path/to/v/file" > path/to/adjlist
