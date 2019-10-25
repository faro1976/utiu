package it.utiu.tapas.base

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.Transformer
import it.utiu.tapas.base.AbstractStatsFeederActor.AskStats
import it.utiu.tapas.base.AbstractStatsFeederActor.TellStats

object AbstractStatsFeederActor {
  case class AskStats()
  case class TellStats(strCSV: String)
}

abstract class AbstractStatsFeederActor(name: String) extends AbstractBaseActor(name) {
  var strCSV: String = null;

  override def receive: Receive = {
    case AskStats() =>
      log.info("stats requested")
      sender ! TellStats(strCSV)
    case AbstractAnalyzerActor.AnalysisFinished(result) =>
      log.info("refreshed stats data just built")
      strCSV = result
  }

}