#export JAVA_HOME=[java_home]
#export HADOOP_HOME=[hadoop_home]
#export HADOOP_STREAMING=[hadoop_streaming_jar]
#export PATH=$PATH:$HADOOP_HOME/bin

#hadoop-master
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/tmp
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/namenode
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/datanode
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/hadoop-2.9.0/logs/*
#rob-VirtualBox
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/tmp
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/datanode
sudo rm -r /Users/robertofavaroni/src/extlib/hadoop/hadoop-2.9.0/logs/*

hdfs namenode -format
hdfs dfs -mkdir /progetto1
hdfs dfs -put ./scontrini.txt /progetto1

Rscript ./job[x].R
