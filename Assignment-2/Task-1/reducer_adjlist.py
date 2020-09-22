#!/usr/bin/python3
import sys

v_file_dest = sys.argv[1]
dict = {}
current_node = None
outlinks = []
node = None

with open(v_file_dest, "w") as file:
    
    for line in sys.stdin:
        line = line.strip()
        node, outlink = line.split()
        try:
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
                file.write("%s,%d\n" % (node, 1))
            if outlink not in dict:
                dict[outlink] = 1
                file.write("%s,%d\n" % (outlink, 1))
        except:
            continue

if current_node == node:
    comma_sep_value = ",".join(outlinks)
    print(current_node, comma_sep_value)
