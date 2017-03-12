#! /bin/bash

chmod 775 /home/cloudera/mapper.py
chmod 775 /home/cloudera/reducer.py
chmod 777 /home/cloudera/

# Copy input files in the hadoop file system
hdfs dfs -mkdir /user/cloudera/input_PageRank/
hdfs dfs -put /home/cloudera/input_pagerank.txt  /user/cloudera/input_PageRank/

# Iterations
for ((i=1; i <= $1 ; i++))
do
	hdfs dfs -rm -r /user/cloudera/output_PageRank/
	hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -input /user/cloudera/input_PageRank/ -output /user/cloudera/output_PageRank/ -mapper '/home/cloudera/mapper.py '$i' '$2'' -reducer '/home/cloudera/reducer.py '$3''
	echo 'Iteration '$i' completed.'
done

# Get the output to local path
hdfs dfs -get /user/cloudera/output_PageRank/ /home/cloudera/
hdfs dfs -rm -r /user/cloudera/output_PageRank/
