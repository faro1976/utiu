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
import com.typesafe.config.ConfigFactory

object AbstractBaseActor {
  //static application params
  val HDFS_URL = "hdfs://localhost:9000/"
  val KAFKA_BOOT_SVR = "localhost:9092"
  val KAFKA_GROUP_ID = "group1"
  val LOOP_DELAY = 300000
}

abstract class AbstractBaseActor(name: String) extends Actor with ActorLogging {
  //set URLs  
  val SPARK_URL_TRAINING = context.system.settings.config.getString("tapas.spark.trainer")
  val SPARK_URL_PREDICTION = context.system.settings.config.getString("tapas.spark.predictor")
  val SPARK_URL_ANALYSIS = context.system.settings.config.getString("tapas.spark.analyzer")
  
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
  val tmstFormat = new SimpleDateFormat("yyMMdd HH:mm")
  
   override def supervisorStrategy = OneForOneStrategy() {
      case _: Exception => SupervisorStrategy.Restart
   }    
  
  protected def writeFile(file: String, text: String, mode: Option[StandardOpenOption]) {
    val path = Paths.get(file)
    if (!Files.exists(path)) Files.createFile(path) 
    if (mode.isDefined) Files.write(path, text.toString.getBytes, mode.get) else  Files.write(path, text.toString.getBytes)         
  }
}