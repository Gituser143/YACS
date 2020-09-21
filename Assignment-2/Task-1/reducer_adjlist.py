#!/usr/bin/python3
import sys

current_node = None
outlinks = []
node = None

for line in sys.stdin:
    line = line.strip()
    node, outlink = line.split()
    try:
        if current_node == node:
            outlinks.append(outlink)
        else:
            if current_node:
                comma_sep_value = ",".join(outlinks)
                print(current_node, comma_sep_value)
            outlinks = [outlink]
            current_node = node
    except:
        continue

if current_node == node:
    comma_sep_value = ",".join(outlinks)
    print(current_node, comma_sep_value)
