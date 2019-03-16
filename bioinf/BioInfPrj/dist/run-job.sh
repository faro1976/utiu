$SPARK_HOME/bin/spark-submit --class it.utiu.bioinf.Runner --master spark://localhost:7077 --executor-memory 4g --total-executor-cores 2 target/BioInfPrj-0.0.1-SNAPSHOT.jar
#~/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class it.utiu.bioinf.Runner --master spark://localhost:7077 --executor-memory 25g --total-executor-cores 8 ~/BioInfPrj-0.0.1-SNAPSHOT.jar
 
