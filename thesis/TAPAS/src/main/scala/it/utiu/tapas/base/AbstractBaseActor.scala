package it.utiu.tapas.base

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat

import AbstractBaseActor.HDFS_URL
import akka.actor.Actor
import akka.actor.ActorLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object AbstractBaseActor {
  //static application params
  val HDFS_URL = "hdfs://localhost:9000/"
  val KAFKA_BOOT_SVR = "localhost:9092"
  val KAFKA_GROUP_ID = "group1"
  val LOOP_DELAY = 600000
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

  //Spark objects
  var conf: SparkConf = null
  var spark: SparkSession = null
  var sc: SparkContext = null

  protected def initSpark(task: String, url: String) {
    //Spark Configuration
    conf = new SparkConf()
      .setAppName(name + "-" + task)
      .setMaster(url)
      .set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

    //Spark Session
    spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    //Spark Context
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  }

  protected def writeFile(file: String, text: String, mode: Option[StandardOpenOption]) {
    val path = Paths.get(file)
    if (!Files.exists(path)) Files.createFile(path)
    if (mode.isDefined) Files.write(path, text.toString.getBytes, mode.get) else Files.write(path, text.toString.getBytes)
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    println("preRestart " + name + " for " + reason)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) = {
    println("postRestart " + name + " for " + reason)
    super.postRestart(reason)
  }

  override def preStart() = {
    log.info("preStart " + name)
    super.preStart()
  }
  override def postStop() = {
    log.info("postStop " + name)
    super.postStop()
  }
}