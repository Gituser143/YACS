#!/usr/bin/python3
import sys

dict = {}
with open("v", "r") as v_file:
    line = v_file.readline().strip()
    v_node, rank = line.split(",")
    dict[v_node] = float(rank)

for line in sys.stdin:
    try:
        node, outlinks = line.split()
        outlinks = outlinks.split(",")
        rank = dict[node]
        contribution = rank/len(outlinks)

        for i in outlinks:
            print(i, contribution)

    except:
        continue
