package it.utiu.tapas.base

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.SparkConf
import it.utiu.tapas.base.AbstractPredictorActor._
import java.util.Date
import org.apache.spark.ml.classification.LogisticRegressionModel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import org.apache.commons.io.FileUtils
import java.io.File


object AbstractPredictorActor {
  case class AskPrediction(msgs: String)
  case class TellPrediction(prediction: String)

}
abstract class AbstractPredictorActor[T <: Model[T]](name: String) extends AbstractBaseActor(name) {
  var lrModel: Model[T] = null
  
  override def receive: Receive = {

    case AskPrediction(msgs: String) =>
      sender ! TellPrediction(doPrediction(msgs))
      
    case AbstractTrainerActor.TrainingFinished() =>
      println("loading model "+ML_MODEL_FILE_COPY+" ...")
      FileUtils.deleteDirectory(new File(ML_MODEL_FILE_COPY))
      FileUtils.copyDirectory(new File(ML_MODEL_FILE), new File(ML_MODEL_FILE_COPY), true);
//      Files.copy(StandardCopyOption.REPLACE_EXISTING)
      lrModel = getAlgo().load(ML_MODEL_FILE_COPY)
      println("loaded model "+ML_MODEL_FILE_COPY)
  }

  
  
  def doInternalPrediction(msgs: String, spark: SparkSession, model: Model[T]): String
  def getAlgo(): MLReader[T]
  
  
  private def doPrediction(msgs: String): String = {
    if (lrModel==null) return "ML model not created yet!"    
    
    val conf = new SparkConf().setAppName(name+"-prediction")
      .setMaster(AbstractBaseActor.SPARK_URL_PREDICTION)
      
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")        
    
    val prediction = doInternalPrediction(msgs, spark, lrModel)    

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??    
//    spark.stop()
    
    return prediction
  }  
}