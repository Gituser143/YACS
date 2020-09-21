#!/usr/bin/python3
import sys

dict = {}
v_file = open("v", "r")
nodes = v_file.readlines()
for node_line in nodes:
    v_node, rank = node_line.split(",")
    dict[v_node] = float(rank)

for line in sys.stdin:
    
    try:
        node, outlinks = line.split()
        outlinks = outlinks.split(",")
        outlinks = [int(i) for i in outlinks]

        for v_node in dict:
            if v_node == node:
                    for i in outlinks:
                        print(i, dict[v_node]/len(outlinks))
                        break
    except:
        continue
