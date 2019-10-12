package it.utiu.tapas.base

import org.apache.spark.SparkConf
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.base.AbstractTrainerActor.StartTraining
import it.utiu.tapas.base.AbstractTrainerActor.TrainingFinished
import org.apache.spark.ml.Model
import org.apache.spark.ml.Transformer
import java.text.SimpleDateFormat
import org.apache.hadoop.hdfs.DistributedFileSystem


object AbstractTrainerActor {
  //start training message
  case class StartTraining()
  //finished training message  
  case class TrainingFinished(model: Transformer)
}


abstract class AbstractTrainerActor[T <: Model[T]](name: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {

    case StartTraining() =>
      doTraining()

    case TrainingFinished(model: Model[T]) =>
      log.info("training restart waiting...")
      Thread.sleep(60000)
      log.info("restart training")
      doTraining()
  }
  
  def doInternalTraining(sc: SparkSession): Transformer
  
  private def doTraining() {
    log.info("start training for "+name+"...")
    
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
    val ml = doInternalTraining(spark)
    
    //save ml model
    log.info("saving ml model into " + ML_MODEL_FILE + "...")
    ml.asInstanceOf[MLWritable].write.overwrite().save(ML_MODEL_FILE)
    log.info("saved ml model into " + ML_MODEL_FILE + "...")

    //terminate context
    //spark.stop()

    //notify predictor forcing model refresh
    context.actorSelection("/user/predictor-" + name /*+"*"*/ ) ! TrainingFinished(ml)

    //self-message to start a new training
    self ! AbstractTrainerActor.TrainingFinished(ml)
  }

  
}