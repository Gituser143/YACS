#!/usr/bin/python3
"""reducer.py"""

import sys

dict = {}
dict["1"] = 0
dict["2"] = 0
for line in sys.stdin:

    line = line.strip()
    try:
        word, count = line.split()
        if word not in dict:
            dict[word] = 1
        elif word in dict:
            dict[word] += 1

    except:
        continue

print(dict["1"])
print(dict["2"])
# Command for execution
# hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -mapper "/vagrant/UE18CS322-BD/Assignment-1/Task-1/mapper.py aircraft carrier" -reducer "/vagrant/UE18CS322-BD/Assignment-1/Task-1/reducer.py" -input /user/BD_Assignment1/plane_carriers.ndjson -output /user/BD_Assignment1/output
