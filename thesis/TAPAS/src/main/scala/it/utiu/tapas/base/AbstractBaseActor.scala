package it.utiu.tapas.base

import akka.actor.Actor
import akka.actor.ActorLogging
import AbstractBaseActor._

object AbstractBaseActor {
  //costanti applicative
  val SPARK_URL_TRAINING = "spark://localhost:8001"
  val SPARK_URL_PREDICTION = "spark://localhost:8002"
  val HDFS_URL = "hdfs://localhost:9000/"
  val kafkaBootstrapServers = "localhost:9092"
  val groupId = "group1"
}

abstract class AbstractBaseActor(name: String) extends Actor with ActorLogging {
  val HDFS_CS_PATH=HDFS_URL+"/"+name+"/"
  val ML_MODEL_FILE = HDFS_URL+"ml-model/"+name+"/"
  val RT_INPUT_FILE = "./input/"+name+".input"
}