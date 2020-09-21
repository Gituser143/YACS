#!/usr/bin/python3
import sys

current_node = None
outlinks = []
node = None
for line in sys.stdin:
    line = line.strip()
    if line != '':
        if line[0] != '#':
            node, outlink = line.split()
            if current_node == node:
                outlinks.append(outlink)
            else:
                if current_node:
                    comma_sep_value = ",".join(outlinks)
                    print(current_node, comma_sep_value)
                outlinks = [outlink]
                current_node = node

if current_node == node:
    comma_sep_value = ",".join(outlinks)
    print(current_node, comma_sep_value)
