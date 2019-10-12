package it.utiu.tapas.base

import AbstractBaseActor.HDFS_URL
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.nio.file.Files
import java.text.SimpleDateFormat

object AbstractBaseActor {
  //static application params
  val SPARK_URL_TRAINING = "spark://localhost:8001"
  val SPARK_URL_PREDICTION = "spark://localhost:8002"
  val HDFS_URL = "hdfs://localhost:9000/"
  val KAFKA_BOOT_SVR = "localhost:9092"
  val KAFKA_GROUP_ID = "group1"
}

abstract class AbstractBaseActor(name: String) extends Actor with ActorLogging {
  //dynamic application params
  val HDFS_CS_PATH = HDFS_URL + "/" + name + "/"
  val HDFS_CS_INPUT_PATH = HDFS_CS_PATH + "input/"
  
  //file paths
  val ML_MODEL_FILE = "./ml-model/" + name + "/"
  val ML_MODEL_FILE_COPY = "./ml-model/" + name + "_copy/"
  val RT_PATH = "./rt/" + name + "/"
  val RT_INPUT_PATH = RT_PATH + "input/"
  val RT_OUTPUT_PATH = RT_PATH + "output/"
  val RT_OUTPUT_FILE = RT_OUTPUT_PATH + name + "-prediction.csv"
  val ANALYTICS_OUTPUT_FILE = RT_OUTPUT_PATH + name + "-stats.csv"
  
  //date pattern for csv
  val tmstFormat = new SimpleDateFormat("yyMMdd hh:mm")
  
   override def supervisorStrategy = OneForOneStrategy() {
      case _: Exception => SupervisorStrategy.Restart
   }    
  
  protected def writeFile(file: String, text: String, mode: StandardOpenOption) {
    val path = Paths.get(file)
    if (!Files.exists(path)) Files.createFile(path) 
    Files.write(path, text.toString.getBytes, mode)    
  }
}