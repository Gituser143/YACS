#!/usr/bin/python3
import sys

dict = {}

for line in sys.stdin:
    line = line.strip()
    if line != '':
        if line[0] != '#':
            try:
                key, value = line.split()
                if key not in dict:
                    dict[key] = []
                    dict[key].append(int(value))
                elif key in dict:
                    dict[key].append(int(value))
            except:
                continue

for key, value in dict.items():
    print(key, end=" ")
    str_value = [str(i) for i in value]
    comma_sep_value = ",".join(str_value)
    print(comma_sep_value)
