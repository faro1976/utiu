package it.utiu.tapas.ml.predictor

import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import it.utiu.tapas.util.Consts
import it.utiu.tapas.ml.predictor.WineForecasterActor.TellPrediction
import it.utiu.anavis.WineTrainerActor

object WineForecasterActor {

  def props(): Props = Props(new WineForecasterActor())

  case class AskPrediction(msgs: List[String])
  case class TellPrediction(prediction: List[String])
}

class WineForecasterActor() extends Actor with ActorLogging {

  override def receive: Receive = {

    case WineForecasterActor.AskPrediction(msgs: List[String]) =>
      println("prediction starting...")
      sender ! TellPrediction(doPredict(msgs))
  }

  def doPredict(msgs: List[String]): List[String] = {

    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    
    println("features from loaded model " + LogisticRegressionModel.read.load(WineTrainerActor.MODEL_PATH).numFeatures)
    val lrModel: LogisticRegressionModel = LogisticRegressionModel.read.load(Consts.MODEL_PATH)

    val sentenceData = spark.createDataFrame(Seq(
      msgs)).toDF()
    val assembler = new VectorAssembler().setInputCols(Array("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)

    val predictions = lrModel.transform(ds)
    println("prediction from loaded model...")
    predictions.show()
    val ret = predictions.select("predictedClass").collect().map(_(0)).toList

    //terminazione contesto
    spark.stop()

    return ret.asInstanceOf[List[String]]
  }

}