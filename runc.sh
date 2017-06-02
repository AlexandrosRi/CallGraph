#!/usr/bin/env bash

if [ $# -eq  0 ]
then
# compile program
    echo "compiling"
    #sbt update
    sbt clean
    sbt package
    echo "compilation finished!"
fi

# clear files from previous executions
rm -rf ~/dpl/HDFSoutput  > /dev/null 2>&1
# hdfs dfs -rm -R -skipTrash input > /dev/null 2>&1
# hdfs dfs -rm -R -skipTrash outputSeq > /dev/null 2>&1
hdfs dfs -rm -R -skipTrash output > /dev/null 2>&1

# create the input directory and upload data
# hdfs dfs -mkdir input
# hdfs dfs -put ../input/* input

# execute the program
T="$(date +%s)"

spark-submit \
  --class "com.mscis.CGA.CGGen" \
  --master spark://172.16.10.34:7077 \
  --executor-memory 4G \
  --jars libs/javaparser-core-3.2.0.jar \
  target/scala-2.11/callgraph_2.11-1.0.jar

T="$(($(date +%s)-T))"
echo "Program execution took $T seconds!"

# get the results to local directory
hdfs dfs -get output/test ~/dpl/HDFSoutput
