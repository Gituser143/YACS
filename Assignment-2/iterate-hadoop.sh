#!/bin/sh
CONVERGE=1
rm v* log*

$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
hdfs dfs -rm -r /output* 

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-mapper "/vagrant/UE18CS322-BD/Assignment-2/Task-1/mapper_adjlist.py " \
-reducer "/vagrant/UE18CS322-BD/Assignment-2/Task-1/reducer_adjlist.py '/vagrant/UE18CS322-BD/Assignment-2/v'"  \
-input /dataset-A2 \
-output /output1 #has adjacency list


while [ "$CONVERGE" -ne 0 ]
do
	$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
	-mapper "/vagrant/UE18CS322-BD/Assignment-2/Task-2/mapper_pagerank.py '/vagrant/UE18CS322-BD/Assignment-2/v' " \
	-reducer /vagrant/UE18CS322-BD/Assignment-2/Task-2/reducer_pagerank.py \
	-input /output1 \
	-output /output2
	touch v1
	hadoop fs -cat /output2/* > /vagrant/UE18CS322-BD/Assignment-2/v1
	CONVERGE=$(python3 check_conv.py >&1)
	hdfs dfs -rm -r /output2
	echo $CONVERGE

done
