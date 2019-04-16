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
import org.apache.spark.sql.catalyst.expressions.IsNaN
import java.text.DecimalFormat


object Runner {
  //costanti applicative
//  val PATH = "hdfs://localhost:9000/bioinf/"  //HDFS path
  val PATH="/Users/rob/tmp/blockchain/"
  val sdf = new SimpleDateFormat("yyyyMMddhh")
//  val sdfm = new SimpleDateFormat("yyMMddhhmm")
  val decF = new DecimalFormat("#.####");

  
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
      new Transaction(jsonTx.get("hash").getAsString, new Date(jsonTx.get("block_timestamp").getAsLong*1000), jsonTx.get("input_count").getAsInt, jsonTx.get("output_count").getAsInt, jsonTx.get("fee").getAsDouble, jsonTx.get("output_value").getAsDouble, jsonTx.get("size").getAsLong)
    }).cache()
//    //estrazione csv dati non aggregati delle transazioni limitatamente a un determinato sottoperiodo
//    //riga csv: timestamp, hash, fee, outputValue, size
//    var lst0 = new ListBuffer[Array[String]]()
//    //header
//    lst0.append(Array("date", "hash", "fee", "output_value", "size"))
//    //trasporto su memoria driver dell'intero dataset delle transazioni
//    rddTxs.take(10).foreach(t=>println(t.fee))
//    rddTxs.collect().foreach(t=>lst0.append(Array(sdf.format(t.timestamp), t.hash, t.fee.toString, t.outputValue.toString, t.size.toString)))
//    writeCSV("csv/allTransactions.csv", lst0.toList)
    
     
//    rddTxs.collect().foreach(println)
    println("total number of transactions: "+rddTxs.count)

    //restituisce RDD: numero transazioni confermate in blockchain, volume transato totale, volume transato medio, fee media
    val rddDailyStats = rddTxs.map(t=>(sdf.format(t.timestamp), t)).groupByKey().map(t=>(t._1, (t._2.size.toDouble, t._2.map(t2=>t2.outputValue).sum.toDouble,  t._2.map(t2=>t2.outputValue).sum/t._2.size, t._2.map(t2=>t2.outputValue).sum/t._2.size)))
     

    
    //caricamento dati pricing Bitcoin e aggregazione con media per data 
    //restituisce RDD: timestamp, price
    var rddDailyPriceInit = sc.textFile(PATH+"coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv")
    val header = rddDailyPriceInit.first()
    val rddDailyPrice = rddDailyPriceInit.filter(row => row != header)
      .map(line=>{
        val values = line.split(",")
        //restituisce Row: data, prezzo cambio USD
        (sdf.format(new Date(values(0).toLong*1000)), values(4).toDouble)
        }).groupByKey().map(p=>(p._1, p._2.sum/p._2.size))
        
    
    //join transaction statistics and daily price by date
    //resituisce RDD: timestamp, price, total_transactions, total_amount, average_amount, average_fee 
    val rddJoined = rddDailyStats.join(rddDailyPrice)
        .map(e=>Row(e._1,e._2._2,e._2._1._1,e._2._1._2,e._2._1._3,e._2._1._4))
                
    
    val schema = new StructType()
      .add(StructField("date", StringType, false))
      .add(StructField("price", DoubleType, false))
      .add(StructField("total_transactions", DoubleType, false))
      .add(StructField("total_amount", DoubleType, false))
      .add(StructField("average_amount", DoubleType, false))
      .add(StructField("average_fee", DoubleType, false))      
      
    val df = spark.createDataFrame(rddJoined, schema).na.drop()
    
    var lstStats = new ListBuffer[Array[String]]()
    //header
    lstStats.append(Array("date", "price", "total_transactions", "total_amount", "average_amount", "average_fee"))    
    df.sort("date").collect().foreach(r=>lstStats.append(Array(r.get(0).toString(),decF.format(r.get(1)),r.get(2).toString(),r.get(3).toString(),decF.format(r.get(4)),decF.format(r.get(5)))))    
    writeCSV("web/csv/dailyStats.csv", lstStats.toList)
    
    val assembler = new VectorAssembler().setInputCols(Array("total_transactions","total_amount","average_amount","average_fee")).setOutputCol("features")
    val dfML = assembler.transform(df).cache()
    val Array(training, test) = dfML.randomSplit(Array(0.7, 0.3), 123)
    println("training count:"+training.count())
    println("test count:"+test.count())
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
    
    
    val predictions = lrModel.transform(test)
    predictions.show()
    var lstPreds = new ListBuffer[Array[String]]()
    //header
    lstPreds.append(Array("date", "price", "predictedPrice"))    
    predictions.sort("date").collect().foreach(r=>lstPreds.append(Array(r.get(0).toString(),decF.format(r.get(1)),decF.format(r.get(7)))))    
    writeCSV("web/csv/predictions.csv", lstPreds.toList)
    
    
    spark.stop()
    
  }
  
  private def writeCSV(fileName: String, values: List[Array[String]]) {
    val outputFile = new BufferedWriter(new FileWriter("./"+fileName))
    val csvWriter = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)
    csvWriter.writeAll(values)
    outputFile.close()
  }
}