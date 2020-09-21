#!/usr/bin/python3
import sys

for line in sys.stdin:
    node, outlinks = line.split()
    outlinks = outlinks.split(",")
    outlinks = [int(i) for i in outlinks]
    with open("v.txt", "r") as file:
        while True:
            file_line = file.readline().strip()
            v_node, rank = file_line.split(",")
            if v_node == node:
                for i in outlinks:
                    print(i, rank/len(outlinks))
                    break
            if not file_line:
                break
