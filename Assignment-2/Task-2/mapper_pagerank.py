#!/usr/bin/python3
""" mapper_adjlist.py """

import sys

for line in sys.stdin:
    try:
        node, outlinks = line.split()
        outlinks = outlinks.split(",")
        outlinks = [int(i) for i in outlinks]
        with open("v", "r") as file:
            while True:
                file_line = file.readline().strip()
                v_node, rank = file_line.split(",")
                if v_node == node:
                    for i in outlinks:
                        print(i, rank/len(outlinks))
                    break
                if not file_line:
                    break
    except:
        continue
