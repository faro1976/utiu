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
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

object WineForecasterActor {
val SPARK_URL = "spark://localhost:7077"
  def props(): Props = Props(new WineForecasterActor())

  case class AskPrediction(msgs: String)
  case class TellPrediction(prediction: List[String])
}

class WineForecasterActor() extends Actor with ActorLogging {

  override def receive: Receive = {

    case WineForecasterActor.AskPrediction(msgs: String) =>
      println("prediction starting...")
      sender ! TellPrediction(doPredict(msgs))
  }

  def doPredict(msgs: String): List[String] = {
    
    
    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
      .set("spark.executor.instances", "1")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "1g")
      
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
        
    val lrModel = LogisticRegressionModel.read.load(WineTrainerActor.MODEL_PATH)
    println("features from loaded model " + lrModel.numFeatures)
    
    //cast to List[List[Double]]
    val buffInput = new ListBuffer[List[Double]]()
//    msgs.foreach(m=>buffInput.append(m.split(",").map(_.).toList))    
//    val input :List[List[Double]] =buffInput.toList 
//    val input = msgs.split(",").map(_.).toList
    val tokens =msgs.split(",")
    val input = (tokens(0).toDouble,tokens(1).toDouble,tokens(2).toDouble,tokens(3).toDouble,tokens(4).toDouble,tokens(5).toDouble,tokens(6).toDouble,tokens(7).toDouble,tokens(8).toDouble,tokens(9).toDouble,tokens(10).toDouble,tokens(11).toDouble,tokens(12).toDouble)
    
    val d1 = List(13.39, 1.77, 2.62, 16.1, 90, 2.85, 2.94, .34, 1.45, 4.8, .92, 3.22, 1009)
    val d2 = List(12.79, 2.67, 2.45, 22, 112, 1.48, 1.36, .24, 1.26, 10.8, .48, 1.47, 344)
    val d3 = List(12.15, 2.67, 2.43, 22, 112, 1.48, 1.36, .24, 1.26, 10.8, .48, 1.47, 344)

    val rob = List(d1, d2, d3)
    
    
//    val values1 = List(List("1", "One") ,List("2", "Two") ,List("3", "Three"),List("4","4")).map(x =>(x(0), x(1)))
    val values1 = List("1","2","3","4")

import spark.implicits._

val someDF = Seq(
  (8, "bat"),
  (64, "mouse"),
  (-27, "horse")
).toDF("number", "word")

//val df1 = values1.toDF("a","b","c","d")   
//    val sentenceData = spark.createDataFrame(Seq(
//      rob)).toDF("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")
    
    val sentenceData = Seq(input).toDF("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")
    val assembler = new VectorAssembler().setInputCols(Array("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)
    ds.show()

    val predictions = lrModel.transform(ds)
    println("prediction from loaded model...")
    predictions.show()
    val ret = predictions.select("predictedClass").collect().map(_(0)).toList

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??    
//    spark.stop()

    return ret.asInstanceOf[List[String]]
  }

}