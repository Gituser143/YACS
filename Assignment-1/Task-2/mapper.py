import sys


# Checks for command ine args
if len(sys.argv) != 3:
    # Set default if not given
    word = "aircraft carrier"
    distance = 0
else:
    word = sys.argv[1]
    distance = sys.argv[2]


for line in sys.stdin:
    print('Do mapper kelsa with', word, distance)
