#!/usr/bin/python3
import sys

v_file_path = sys.argv[1]
dict = {}
check = {}
with open(v_file_path, "r") as v_file:
    while True:
        line = v_file.readline()
        if not line:
            break
        v_node, rank = line.strip().split(",")
        dict[v_node] = float(rank)
        check[v_node] = 0

for line in sys.stdin:
    try:
        node, outlinks = line.split()
        outlinks = outlinks.split(",")
        rank = dict[node]
        contribution = rank/len(outlinks)

        for i in outlinks:
            print(i, contribution)
            if i in check:
                del check[i]

    except:
        continue

for key in check:
    print(key, check[key])

# command to run locally
# cat path/to/adjlist | ./mapper_pagerank.py "path/to/v/file" | sort -k1,1 | ./reducer_pagerank.py > path/to/v1
