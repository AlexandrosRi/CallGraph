#!/usr/bin/env bash

# compile program
#sbt update
sbt clean
sbt package

# clear files from previous executions
# rm -rf output > /dev/null 2>&1
# hdfs dfs -rm -R -skipTrash outputSeq > /dev/null 2>&1
# hdfs dfs -rm -R -skipTrash outputTxt > /dev/null 2>&1

# create the input directory and upload data
# hdfs dfs -mkdir input
# hdfs dfs -put ../input/* input

# execute the program
T="$(date +%s)"

spark-submit \
  --class "com.mscis.CGA.CGGen" \
  --master local[2] \
  --jars libs/javaparser-core-3.2.0.jar \
  target/scala-2.11/callgraph_2.11-1.0.jar

T="$(($(date +%s)-T))"
echo "Program execution took $T seconds!"

# get the results to local directory
# hdfs dfs -get outputTxt output