package it.utiu.tapas.base

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.SparkConf
import it.utiu.tapas.base.AbstractPredictorActor._


object AbstractPredictorActor {
  case class AskPrediction(msgs: String)
  case class TellPrediction(prediction: String)

}
abstract class AbstractPredictorActor[T <: Model[T]](name: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {

    case AskPrediction(msgs: String) =>

      sender ! TellPrediction(doPrediction(msgs))
  }

  
  
  def doInternalPrediction(msgs: String, spark: SparkSession, model: Model[T]): String
  def getAlgo(): MLReader[T]
  
  
  private def doPrediction(msgs: String): String = {
    
    
    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
      
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    val lrModel = getAlgo().load(ML_MODEL_FILE)
    println("loaded model " + lrModel)
    
    val prediction = doInternalPrediction(msgs, spark, lrModel)
    

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??    
//    spark.stop()
    

    return prediction
  }  
}