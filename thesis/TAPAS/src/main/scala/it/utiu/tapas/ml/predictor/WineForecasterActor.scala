package it.utiu.tapas.ml.predictor



import scala.collection.Seq
import scala.collection.immutable.List
import scala.reflect.api.materializeTypeTag

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import akka.actor.ActorRef
import akka.actor.Props
import it.utiu.tapas.util.Consts
import akka.actor.AbstractActor.Receive
import akka.actor.ActorLogging
import akka.actor.Actor


object WineForecasterActor {

    def props(): Props = Props(new WineForecasterActor())

  case class AskPrediction(msgs: List[List[Double]])
  case class TellPrediction(predict: List[String])
}


class WineForecasterActor()   extends Actor with ActorLogging {

    override def receive: Receive = {


        case WineForecasterActor.AskPrediction(msgs: List[List[Double]]) =>
          println("entro")
          doPredict(msgs)
    }
    
    def doPredict(msgs: List[List[Double]]) {

  
  val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    val MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/wine-ml-model"
    println("features from loaded model " + LogisticRegressionModel.read.load(MODEL_PATH).numFeatures)
    val lrModel: LogisticRegressionModel = LogisticRegressionModel.read.load(Consts.MODEL_PATH)


val sentenceData = spark.createDataFrame(Seq(
  msgs
)).toDF("_1","_2","_3","_4","_5","_6","_7","_8","_9","_10","_11","_12","_13","_14")
    
    val assembler = new VectorAssembler().setInputCols(Array("_1","_2","_3","_4","_5","_6","_7","_8","_9","_10","_11","_12","_13","_14")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)
    
    

    val predictions = lrModel.transform(ds)
    println("prediction from loaded model...")
    predictions.show()
    
    //terminazione contesto
    spark.stop()
  }

}