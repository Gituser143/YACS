#!/usr/bin/python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        from_node, to_node = line.split()
        print(from_node, to_node)
    except:
        continue
