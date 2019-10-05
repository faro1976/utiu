package it.utiu.tapas.base

import org.apache.spark.SparkConf
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.base.AbstractTrainerActor.StartTraining
import it.utiu.tapas.base.AbstractTrainerActor.TrainingFinished
import org.apache.spark.ml.Model
import org.apache.spark.ml.Transformer


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
      println("training restart waiting...")
      Thread.sleep(60000)
      println("restart training")
      doTraining()
  }
  
  def doInternalTraining(sc: SparkSession): Transformer
  
  private def doTraining() {
    log.info("start training for "+name+"...")
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
    val ml = doInternalTraining(spark)
    
    //save ml model
    println("saving ml model into " + ML_MODEL_FILE + "...")
    ml.asInstanceOf[MLWritable].write.overwrite().save(ML_MODEL_FILE)
    println("saved ml model into " + ML_MODEL_FILE + "...")

    //terminate context
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??
    //spark.stop()

    //notify predictor forcing model refresh
    context.actorSelection("/user/predictor-" + name /*+"*"*/ ) ! TrainingFinished(ml)

    //self-message to start a new training
    self ! AbstractTrainerActor.TrainingFinished(ml)
  }

  
}