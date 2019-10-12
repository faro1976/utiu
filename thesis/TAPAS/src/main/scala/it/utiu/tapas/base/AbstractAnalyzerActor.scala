package it.utiu.tapas.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import org.apache.spark.sql.Row
import it.utiu.tapas.base.AbstractAnalyzerActor.StartAnalysis
import it.utiu.tapas.base.AbstractAnalyzerActor.AnalysisFinished
import scala.collection.mutable.ArrayBuffer
import it.utiu.tapas.base.AbstractAnalyzerActor.AskAnalytics
import it.utiu.tapas.base.AbstractAnalyzerActor.TellAnalytics

object AbstractAnalyzerActor {
  case class StartAnalysis()
  case class AnalysisFinished()
  case class AskAnalytics()
  case class TellAnalytics(strCSV: String)
}

abstract class AbstractAnalyzerActor(name: String) extends AbstractBaseActor(name) {
  var strCSV: String = null;  
  
  override def receive: Receive = {

    case StartAnalysis() => doAnalysis()
    
    case AnalysisFinished() =>
      log.info("analysis restart waiting...")
      Thread.sleep(60000)
      log.info("restart analysis")
      doAnalysis()
    
    case AskAnalytics => sender ! TellAnalytics(strCSV)
  }

  //(List[header_col], List[time, List[value]])
  def doInternalAnalysis(spark: SparkSession): (Array[String], List[Row])

  private def doAnalysis() {
    log.info("start analysis...")
    //Spark Configuration
    val conf = new SparkConf()
      .setAppName(name + "-training")
      .setMaster(AbstractBaseActor.SPARK_URL_TRAINING)
      .set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem")
      
    //Spark Session  
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
      
    //Spark Context  
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //invoke internal
    val stats = doInternalAnalysis(spark)
    //format csv
    val buff = new StringBuilder()
    //csv header
    buff.append(stats._1.mkString(",")+"\n")
    //csv values
    stats._2.foreach(row=>
      buff.append(row.toSeq.mkString(",")+"\n")
    )
    log.info("stats computed:\n"+buff)    
    strCSV = buff.toString
    
    //terminate context
    //spark.stop()

    //self-message to start a new training
    self ! AbstractAnalyzerActor.AnalysisFinished()
  }
}