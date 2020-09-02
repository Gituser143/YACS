#!/usr/bin/python3
""" mapper.py """

import sys
import datetime 


f = open("../plane_carriers.ndjson","r")
lines = f.readlines()

def is_weekend(date):
    day = datetime.datetime.strptime(date, '%Y-%m-%d').weekday()
    if (day == 5 or day == 6):
        return 1
    else:
        return 0

def check(type,x) : 
    if(type == 0):
        word_list = x.split()
        for element in word_list : 
            if not element.isalpha() : 
                return 0

    elif(type == 1):
        if len(x) !=2 : 
            return 0
        else:
            if not (ord(x[0]) >= 133 or ord(x[0]) <= 100) : 
                return 0
            if not (ord(x[1]) >= 133 or ord(x[1]) <= 100) : 
                return 0

    elif(type == 2):
        if len(x.split()) != 2:
            return 0

    elif(type == 3):
        if x not in ["true","false"]:
            return 0

    return 1

for line in lines:

    i = 0
    finallist = []
    line = line.strip().strip("{").strip("}").split(", ")

    while i < 4:

        x = (line[i].split(":")[1]).strip(" ").strip('"')
        if(check(i,x) ==0) : 
            continue
        finallist.append(str(x))
        i +=1

    if(len(finallist) == 4):

        if finallist[0] == "aircraft carrier":

            if (finallist[3] == 'true'):
                print(1," ",1)
                pass
            
            elif (finallist[3] == 'false') & is_weekend(finallist[2].split()[0]):
                print(2," ",1)
                pass
