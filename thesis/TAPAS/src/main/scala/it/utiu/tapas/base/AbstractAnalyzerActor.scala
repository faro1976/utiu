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

object AbstractAnalyzerActor {
  case class StartAnalysis()
  case class AnalysisFinished()
}

abstract class AbstractAnalyzerActor(name: String) extends AbstractBaseActor(name) {

  override def receive: Receive = {

    case StartAnalysis() => doAnalysis()
    case AnalysisFinished() =>
      println("analysis restart waiting...")
      Thread.sleep(60000)
      println("restart analysis")
      doAnalysis()
  }

  //(List[header_col], List[time, List[value]])
  def doInternalAnalysis(spark: SparkSession): (Array[String], List[Row])

  private def doAnalysis() {
    log.info("start analysis...")
    //start Spark session
    val conf = new SparkConf()
      .setAppName(name + "-training")
      .setMaster(AbstractBaseActor.SPARK_URL_TRAINING)
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
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
    println(buff)
    Files.write(Paths.get(ANALYTICS_OUTPUT_FILE), buff.toString.getBytes, StandardOpenOption.CREATE)

    
    //terminate context
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??
    //spark.stop()

    //self-message to start a new training
    self ! AbstractAnalyzerActor.AnalysisFinished()
  }
}