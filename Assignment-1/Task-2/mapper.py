import sys
import json

# Checks for command line args
if len(sys.argv) != 3:
    # Set default if not given
    word = "airplane"
    distance = 0
else:
    word = sys.argv[1]
    distance = sys.argv[2]


for line in sys.stdin:
    #print('Do mapper kelsa with', word, distance)
    line = json.loads(line)

    if all(x.isalpha() or x.isspace() for x in line["word"]):
        if ((len(line["countrycode"]) == 2 and line["countrycode"].isupper()) and (line["recognized"] == True or line["recognized"] == False)  and len(line["key_id"])==16 and len(line["drawing"])>=1):
            euclidean_distance = 0
            
            if(line["word"] == word):
                x0 = line["drawing"][0][0][0]
                y0 = line["drawing"][0][1][0]
                # print(x0, y0)
            
                euclidean_distance += x0*x0 + y0*y0;
                
                if(euclidean_distance > distance*distance):
                    print (line["countrycode"], 1)
            
 
