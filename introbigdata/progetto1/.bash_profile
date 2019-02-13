# Setting PATH for Python 3.6
# The original version is saved in .bash_profile.pysave
PATH="/Library/Frameworks/Python.framework/Versions/3.6/bin:${PATH}"
export PATH

export JAVA_HOME=$(/usr/libexec/java_home)

export HADOOP_HOME=/Users/robertofavaroni/src/extlib/hadoop/hadoop-2.9.0
export HADOOP_STREAMING=/Users/robertofavaroni/src/extlib/hadoop/hadoop-2.9.0/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar
export MONGO_HOME=/Users/robertofavaroni/src/extlib/mongodb-osx-x86_64-3.4.1
export PATH=$PATH:$MONGO_HOME/bin:$HADOOP_HOME/bin

