#!/usr/bin/python3
import sys

v_file_dest = sys.argv[1]
dict = {}
current_node = None
outlinks = []
node = None

for line in sys.stdin:
    try:
        line = line.strip()
        node, outlink = line.split()
        # for adjlist
        if current_node == node:
            outlinks.append(outlink)
        else:
            if current_node:
                comma_sep_value = ",".join(outlinks)
                print(current_node, comma_sep_value)
            outlinks = [outlink]
            current_node = node
        # for v file
        if node not in dict:
            dict[node] = 1
    except:
        continue

if current_node == node:
    comma_sep_value = ",".join(outlinks)
    print(current_node, comma_sep_value)

with open(v_file_dest, "w") as file:
    for key in sorted(dict.keys()):
        file.write("%s,%d\n" % (key, 1))
