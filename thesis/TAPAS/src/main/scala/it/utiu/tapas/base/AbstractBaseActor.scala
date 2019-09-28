package it.utiu.tapas.base

import AbstractBaseActor.HDFS_URL
import akka.actor.Actor
import akka.actor.ActorLogging

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
  //TODO ROB spostare modello su HDFS?? problemi con overwrite??
  //  val ML_MODEL_FILE = HDFS_URL+"ml-model/"+name+"/"
  val ML_MODEL_FILE = "./ml-model/" + name + "/"
  val ML_MODEL_FILE_COPY = "./ml-model/" + name + "_copy/"
  val RT_INPUT_FILE = "./input/" + name + ".input"
  val ANALYTICS_OUTPUT_FILE = "./analytics/" + name + ".csv"
}