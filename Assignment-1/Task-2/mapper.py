#!/usr/bin/python3
import sys
import json


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


# Checks for command line args
if len(sys.argv) != 3:
    # Set default if not given
    word = "airplane"
    distance = 0
else:
    word = sys.argv[1]
    distance = float(sys.argv[2])


for line in sys.stdin:

    try:
        line = json.loads(line)

        checks = 0

        for i in range(5):
            checks += isclean(i, line)

            if checks == 5:
                if(line["word"] == word):
                    x0 = line["drawing"][0][0][0]
                    y0 = line["drawing"][0][1][0]

                    euclidean_distance = x0*x0 + y0*y0
                    if(euclidean_distance > distance*distance):
                        print(line["countrycode"], 1)
    except:
        continue
