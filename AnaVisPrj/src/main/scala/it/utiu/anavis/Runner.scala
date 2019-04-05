package it.utiu.anavis

import java.io.BufferedWriter
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.google.gson.JsonParser

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.StringType

object Runner {
  //costanti applicative
//  var PATH = "hdfs://localhost:9000/bioinf/"  //HDFS path
  var PATH = "/Users/rob/tmp/blockchain2017/"
  val sdf = new SimpleDateFormat("yyMMddhh")
  val sdfm = new SimpleDateFormat("yyMMddhhmm")
  
  val jsonParser = new JsonParser()
  
  def main(args: Array[String]): Unit = {
    //spark init
    val conf = new SparkConf().setAppName("Progetto Big Data Analytics and Visualization")
    .setMaster("local")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    //popolamento RDD con dati delle transazioni Bitcoin in formato json 
    val rddTxs = sc.textFile(PATH + "*.json").map(r => {
      val jsonTx = jsonParser.parse(r).getAsJsonObject
      new Transaction(jsonTx.get("hash").getAsString, new Date(jsonTx.get("block_timestamp").getAsLong*1000), jsonTx.get("input_count").getAsInt, jsonTx.get("output_count").getAsInt, jsonTx.get("fee").getAsDouble, jsonTx.get("output_value").getAsDouble)
    }).cache()
     
//    rddTxs.collect().foreach(println)
    println(rddTxs.count)

    //restituisce RDD: numero transazioni confermate in blockchain, volume transato totale, volume transato medio, fee media
    val rddDailyStats = rddTxs.map(t=>(sdf.format(t.timestamp), t)).groupByKey().map(t=>(t._1, (t._2.size.toDouble, t._2.map(t2=>t2.outputValue).sum.toDouble,  t._2.map(t2=>t2.outputValue).sum/t._2.size, t._2.map(t2=>t2.outputValue).sum/t._2.size)))
    var lst1 = new ListBuffer[Array[String]]()
    //riga csv: timestamp, total_transactions, total_amount, average_amount, fee_average
    rddDailyStats.collect().foreach(t=>lst1.append(Array(t._1, t._2._1.toString(), t._2._2.toString(), t._2._3.toString(), t._2._4.toString)))
    writeCSV("dailyTransactions.csv", lst1.toList)

    
    //restituisce RDD: timestamp, price
    var rddDailyPriceInit = sc.textFile(PATH+"coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv")
    val header = rddDailyPriceInit.first()
    val rddDailyPrice = rddDailyPriceInit.filter(row => row != header)
      .map(line=>{
        val values = line.split(",")
        //restituisce Row: data, prezzo cambio USD
        (sdf.format(new Date(values(0).toLong*1000)), values(4).toDouble)
        }).groupByKey().map(p=>(p._1, p._2.sum/p._2.size))
    var lst2 = new ListBuffer[Array[String]]()        
    rddDailyPrice.collect().foreach(p=>lst2.append(Array(p._1, p._2.toString())))
    writeCSV("dailyPrice.csv", lst2.toList)
    
    //join transaction statistics and daily price by date
    //resituisce RDD: timestamp, price, total_transactions, total_amount, average_amount, average_fee 
    val rddJoined = rddDailyStats.join(rddDailyPrice).map(e=>Row(e._1,e._2._2,e._2._1._1,e._2._1._2,e._2._1._3,e._2._1._4))
    
    val schema = new StructType()
      .add(StructField("timestamp", StringType, false))
      .add(StructField("price", DoubleType, false))
      .add(StructField("totTransactions", DoubleType, false))
      .add(StructField("totAmount", DoubleType, false))
      .add(StructField("avgAmount", DoubleType, false))
      .add(StructField("avgFee", DoubleType, false))
      
    val df = spark.createDataFrame(rddJoined, schema)
    val assembler = new VectorAssembler().setInputCols(Array("totTransactions","totAmount","avgAmount","avgFee")).setOutputCol("features")
    val training = assembler.transform(df).cache
    println(training.count())
    training.show()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setPredictionCol("predictedPrice")
      .setLabelCol("price")

    //Fit the model
    val lrModel = lr.fit(training)

    //Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")    
    
    
    spark.stop()
    
  }
  
  private def writeCSV(fileName: String, values: List[Array[String]]) {
    val outputFile = new BufferedWriter(new FileWriter("./"+fileName))
    val csvWriter = new CSVWriter(outputFile)
    csvWriter.writeAll(values)
    outputFile.close()
  }
}