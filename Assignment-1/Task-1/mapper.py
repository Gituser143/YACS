import sys
import datetime 


f = open("../../../plane_carriers.ndjson","r")
lines = f.readlines()

def is_weekend(i):
    return 1
# print (lines[1])
for line in lines:
    # line = lines[1];
    i = 0
    finallist = []
    line = line.strip().strip("{").strip("}").split(", ")
    while i < 4:
        x = (line[i].split(":")[1]).strip(" ").strip('"')
        finallist.append(str(x))
        i +=1
    print (finallist)
    if finallist[0] == "aircraft carrier":
        if (finallist[3] == 'true'):
            print(finallist[0],1)
        
        elif (finallist[3] == 'false') & is_weekend(finallist[2].split()[0]):
            print(finallist[0],2)