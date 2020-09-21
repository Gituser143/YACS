#!/usr/bin/python3
import sys

dict = {}
with open("v", "r") as v_file:
    while True:
        line = v_file.readline()
        if not line:
            break
        v_node,rank = line.strip().split(",")
        dict[v_node] = float(rank)

for line in sys.stdin:
    
    node, outlinks = line.split()
    outlinks = outlinks.split(",")
    rank = dict[node]
    contribution = rank/len(outlinks)

    for i in outlinks:
        print(i, contribution)
