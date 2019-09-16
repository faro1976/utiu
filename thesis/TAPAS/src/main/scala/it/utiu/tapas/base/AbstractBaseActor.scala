package it.utiu.tapas.base

import akka.actor.Actor
import akka.actor.ActorLogging

object AbstractBaseActor {
  //costanti applicative
  val ML_MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/"
  val SPARK_URL = "spark://localhost:7077"
  val HDFS_URL = "hdfs://localhost:9000/" //HDFS URL
  val HDFS_PATH = HDFS_URL + "/" //HDFS path
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"
}

abstract class AbstractBaseActor(name: String) extends Actor with ActorLogging {
  val ML_MODEL_FILE = "/Users/rob/UniNettuno/dataset/ml-model/"+name
}