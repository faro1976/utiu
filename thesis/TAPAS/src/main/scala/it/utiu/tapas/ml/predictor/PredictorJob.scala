package it.utiu.tapas.ml.predictor

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.util.Consts
import org.apache.spark.ml.regression.LinearRegressionModel
import it.utiu.tapas.util.Consts
import it.utiu.tapas.util.Consts
import scala.collection.JavaConverters._
import java.util.ArrayList
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag



//object PredictorActor {
//
//  //  def props(): Props =
//  //    Props(new PredictorActor())
//
//  case class AskPrediction(msg: String)
//  case class TellPredition(predict: String)
//}

object PredictorActor {

  def main(args: Array[String]): Unit = {
    //override def receive: Receive = {

    //    case AskPrediction(msg:String) =>

    //spark init
    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
      .setMaster("local")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/bitcoin.tx")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/bitcoin.tx")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
val MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/confTime-ml-model"
    val lrModel: LinearRegressionModel = LinearRegressionModel.read.load(MODEL_PATH)

val data = List(34,343434)
val data2:java.util.List[Int] = data
val data3:java.util.List[Int] = new ArrayList
val data4:java.util.List[java.util.List[Int]] = new ArrayList
data3.add(34)
data3.add(3423232)
data4.add(data3)

val someData = Seq(
  Row(8, 34343)
)
//val df = spark.createDataFrame(spark.sparkContext.parallelize(someData), BTCSchema.schema2)
val sentenceData = spark.createDataFrame(Seq(
  (8, 6, 123123),
  (3, 34343, 1111)
)).toDF("fee", "size", "confTime")
    
    val assembler = new VectorAssembler().setInputCols(Array("fee","size")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)
    
    
    //    val df = spark.createDataFrame(rddJoined, BTCSchema.schema).na.drop()
    val predictions = lrModel.transform(ds)
    val extPredictions = predictions
      .withColumn("~predictedCT", predictions.col("predictedConfTime").cast("Decimal(10,0)"))
      .withColumn("sConfTime", col("confTime") / 1000)
      .withColumn("sDiff", abs(col("confTime") - col("predictedConfTime")) / 1000)
      .withColumn("%Diff", col("sDiff") / col("sConfTime") * 100)
      
      extPredictions.show()

    //terminazione contesto
    spark.stop()
  }

}