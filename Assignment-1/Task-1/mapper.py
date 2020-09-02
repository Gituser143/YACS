import sys
import datetime 


f = open("../../../plane_carriers.ndjson","r")
lines = f.readlines()

def is_weekend(date):
    day = datetime.datetime.strptime(date, '%Y-%m-%d').weekday()
    if (day == 5 or day == 6):
        return 1
    else:
        return 0

for line in lines:
    i = 0
    finallist = []
    line = line.strip().strip("{").strip("}").split(", ")
    while i < 4:
        x = (line[i].split(":")[1]).strip(" ").strip('"')
        finallist.append(str(x))
        i +=1
    if finallist[0] == "aircraft carrier":
        if (finallist[3] == 'true'):
            print(finallist[0],1)
            pass
        
        elif (finallist[3] == 'false') & is_weekend(finallist[2].split()[0]):
            print(finallist[0],2)
            pass
