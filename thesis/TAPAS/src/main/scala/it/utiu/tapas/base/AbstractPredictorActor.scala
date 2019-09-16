package it.utiu.tapas.base

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.SparkConf
import it.utiu.tapas.base.AbstractPredictorActor._


object AbstractPredictorActor {
  case class AskPrediction(msgs: String)
  case class TellPrediction(prediction: List[String])

}
abstract class AbstractPredictorActor(name: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {

    case AskPrediction(msgs: String) =>

      sender ! TellPrediction(doPrediction(msgs))
  }

  
  
  def doInternalPrediction(spark: SparkSession, model: Model[A]): String
  def getAlgo(): MLReader
  
  
  private def doPrediction(msgs: String): List[String] = {
    
    
    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
      
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    val lrModel = getAlgo().read.load(ML_MODEL_FILE)
    println("features from loaded model " + lrModel.numFeatures)
    
    val prediction = doInternalPrediction(spark, lrModel)
    

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??    
//    spark.stop()
    

    return prediction
  }  
}