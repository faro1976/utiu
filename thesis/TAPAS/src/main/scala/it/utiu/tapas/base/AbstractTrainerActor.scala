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
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{ DataFrame, SparkSession }
import scala.collection.mutable.ArrayBuffer

object AbstractTrainerActor {
  //start training message
  case class StartTraining()
  //finished training message  
  case class TrainingFinished(model: Transformer)
}


abstract class AbstractTrainerActor(name: String) extends AbstractBaseActor(name) {
    
  //Spark Configuration
  val conf = new SparkConf()
    .setAppName(name + "-training")
    .setMaster(SPARK_URL_TRAINING)
    .set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    .set("fs.file.impl","org.apache.hadoop.fs.LocalFileSystem")
  
  //Spark Session
  val spark = SparkSession.builder
    .config(conf)
    .getOrCreate()
  
  //Spark Context
  protected val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  
  def calculateMetrics(algo: String, predictions: DataFrame, rows: (Long, Long)): Double
  
  override def receive: Receive = {

    case StartTraining() =>
      doTraining()

    case TrainingFinished(model: Transformer) =>
      log.info("training restart waiting...")
      Thread.sleep(AbstractBaseActor.LOOP_DELAY)
      log.info("restart training")
      doTraining()
  }
  
  def doInternalTraining(sc: SparkSession): List[(String, Transformer, DataFrame, (Long, Long))]
  
  private def doTraining() {
    log.info("start training...")
    
    //invoke internal
    val evals = doInternalTraining(spark)
    
    //choose fittest model by r2/accuracy evaluator on regression/classification
    val metrics = ArrayBuffer[(Transformer, Double)]()
    for (eval <- evals) {
      val value = calculateMetrics(eval._1, eval._3, eval._4)
      metrics.append((eval._2, value))
    }
    val fittest = metrics.maxBy(_._2)._1
    
    //save ml model
    log.info("saving ml model into " + ML_MODEL_FILE + "...")
    fittest.asInstanceOf[MLWritable].write.overwrite().save(ML_MODEL_FILE)
    writeFile(ML_MODEL_FILE+".algo", fittest.getClass.getName, None)
//    val pipeline = new Pipeline().setStages(Array(ml))
//    pipeline.getStages(0).write.overwrite.save(ML_MODEL_FILE)
    log.info("saved ml model into " + ML_MODEL_FILE + "...")

    //terminate context
    //spark.stop()

    //notify predictor forcing model refresh
    context.actorSelection("/user/predictor-" + name) ! TrainingFinished(fittest)

    //self-message to start a new training
    self ! AbstractTrainerActor.TrainingFinished(fittest)
  }

  
}