#!/usr/bin/python3
""" mapper.py """

import sys
import datetime
import json

# Read command line args
word = sys.argv[1]


def is_weekend(date):
    day = datetime.datetime.strptime(date, '%Y-%m-%d %X.%f %Z').weekday()
    if (day == 5 or day == 6):
        return 1
    else:
        return 0


def isclean(type, x):

    if type == 0:
        # Word contains only alphabets and spaces
        words = x["word"].split()
        for word in words:
            if word.isalpha():
                continue
            else:
                return 0
        return 1

    if type == 1:
        # Country code contains only 2 letters, both in uppercase
        if (len(x["countrycode"]) == 2 and x["countrycode"].isupper()):
            return 1
        return 0

    if type == 2:
        # Recognized should only contain either true of false
        if x["recognized"] == True or x["recognized"] == False:
            return 1
        return 0

    if type == 3:
        # key_id is 16 characterss
        if len(x["key_id"]) == 16:
            return 1
        return 0
    
     if type == 4:
        # drawing contains atleast one stroke
        if len(x["drawing"]) >= 1:
            return 1
        return 0

for line in sys.stdin:

    try:
        line = json.loads(line)
        checks = 0

        for i in range(4):
            checks += isclean(i, line)

            if checks == 5:
                if(line["word"] == word):
                    if(line["recognized"]):
                        print(1, 1)
                    else:
                        if(is_weekend(line["timestamp"])):
                            print(2, 1)
    except:
        continue
