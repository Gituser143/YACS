#!/bin/sh
CONVERGE=1
rm v* log*

$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
hdfs dfs -rm -r /output* 

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-mapper "/home/ubuntu/model-solutions/Assignment2/Python/mapper_t1.py " \
-reducer "/home/ubuntu/model-solutions/Assignment2/Python/reducer_t1.py '/home/ubuntu/model-solutions/Assignment2/Python/v'"  \
-input /dataset-A2 \
-output /output1 #has adjacency list


while [ "$CONVERGE" -ne 0 ]
do
	$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
	-mapper "/home/ubuntu/model-solutions/Assignment2/Python/mapper_t2.py '/home/ubuntu/model-solutions/Assignment2/Python/v' " \
	-reducer /home/ubuntu/model-solutions/Assignment2/Python/reducer_t2.py \
	-input /output1 \
	-output /output2
	touch v1
	hadoop fs -cat /output2/* > /home/ubuntu/model-solutions/Assignment2/Python/v1
	CONVERGE=$(python3 check_conv.py >&1)
	hdfs dfs -rm -r /output2
	echo $CONVERGE

done
