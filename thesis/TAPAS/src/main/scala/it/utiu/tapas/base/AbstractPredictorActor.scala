package it.utiu.tapas.base

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.base.AbstractPredictorActor.AskPrediction
import it.utiu.tapas.base.AbstractPredictorActor.TellPrediction
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.Transformer


object AbstractPredictorActor {
  //ask prediction message
  case class AskPrediction(msgs: String)
  //tell prediction message
  case class TellPrediction(prediction: String, input: String)
}


abstract class AbstractPredictorActor[T <: Model[T]](name: String) extends AbstractBaseActor(name) {
  var mlModel: Model[T] = null

  
  override def receive: Receive = {
    case AskPrediction(msgs: String) =>
      sender ! TellPrediction(doPrediction(msgs), getInput(msgs))

    case AbstractTrainerActor.TrainingFinished(model: Transformer) =>
      mlModel = model.asInstanceOf[Model[T]];
//      println("loaded model " + ML_MODEL_FILE_COPY)
      log.info("reloaded model " + mlModel + " just built")
  }

  
  def doInternalPrediction(msgs: String, spark: SparkSession, model: Model[T]): String
  def getAlgo(): MLReader[T]
  def getInput(msg: String): String = msg 

  private def doPrediction(msgs: String): String = {
    log.info("prediction requested")
    
    //Spark Configuration
    val conf = new SparkConf().setAppName(name + "-prediction")
      .setMaster(AbstractBaseActor.SPARK_URL_PREDICTION)
      .set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem")      

    //Spark Session 
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
      
    //Spark Context
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    if (mlModel == null)  mlModel = loadModelFromDisk().asInstanceOf[Model[T]] //return "ML model not created yet!"
    
    //invoke internal
    val prediction = doInternalPrediction(msgs, spark, mlModel)

    //terminate context
    //    spark.stop()

    return prediction
  }
  
  
  private def loadModelFromDisk() : Transformer = {
      log.info("restoring model " + ML_MODEL_FILE_COPY + " from disk...")
      //delete old copy-of-model
      FileUtils.deleteDirectory(new File(ML_MODEL_FILE_COPY))
      //create a fresh copy-of-model
      FileUtils.copyDirectory(new File(ML_MODEL_FILE), new File(ML_MODEL_FILE_COPY), true);
      //load copy-of-model
      getAlgo().load(ML_MODEL_FILE_COPY)    
  }  
}