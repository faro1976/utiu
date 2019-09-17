package it.utiu.tapas.base

import org.apache.spark.SparkConf
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession

import it.utiu.tapas.base.AbstractTrainerActor.StartTraining
import it.utiu.tapas.base.AbstractTrainerActor.TrainingFinished

object AbstractTrainerActor {
  case class StartTraining()
  case class TrainingFinished()

}
abstract class AbstractTrainerActor(name: String) extends AbstractBaseActor(name) {

  override def receive: Receive = {

    case StartTraining() =>
      println("entro")
      doTraining()

    case TrainingFinished() =>
      println("restart")
      doTraining()

  }

  private def doTraining() {

    //spark init
    val conf = new SparkConf()
      .setAppName("wine")
      .setMaster(AbstractBaseActor.SPARK_URL)
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ml = doInternalTraining(spark)
    //salvataggio modello su file system
    ml.write.overwrite().save(ML_MODEL_FILE)

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??
    //spark.stop()

    //notify predictor in order to refresh model
    context.actorSelection("/user/consumer"+name+"*") ! TrainingFinished()

    //self-message to start a new training
    self ! AbstractTrainerActor.TrainingFinished()
  }

  def doInternalTraining(sc: SparkSession): MLWritable
}