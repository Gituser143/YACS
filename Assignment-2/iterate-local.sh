#!/bin/sh
CONVERGE=1
rm v*

cat ../../web-Google.txt | ./Task-1/mapper_adjlist.py | sort -k1,1 | ./Task-1/reducer_adjlist.py './v' > adjlist #has adjacency list


while [ "$CONVERGE" -ne 0 ]
do
	cat adjlist | ./Task-2/mapper_pagerank.py './v' | sort -k1,1 | ./Task-2/reducer_pagerank.py > v1
	CONVERGE=$(python3 check_conv.py >&1)
	echo $CONVERGE

done
