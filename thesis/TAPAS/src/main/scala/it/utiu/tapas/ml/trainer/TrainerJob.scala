package it.utiu.tapas.ml

import java.io.BufferedWriter
import java.io.FileWriter
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import com.google.gson.JsonParser
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.Month
import java.time.LocalDateTime
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import org.bson.Document
import com.mongodb.client.MongoDatabase
import com.mongodb.MongoClient
import org.apache.spark.ml.regression.LinearRegressionModel
import it.utiu.tapas.util.BTCSchema
import com.mongodb.spark.config.ReadConfig

import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import java.util.List
import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.bson.conversions.Bson




object TrainerJob {
  //costanti applicative
  val PATH = "hdfs://localhost:9000/bitcoin/"  //HDFS path
//  val sdfm = new SimpleDateFormat("yyMMddhhmm")
  val decF = new DecimalFormat("#.####");  
  val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
  val sdf3 = new SimpleDateFormat("HH")
  
  val dateFrom = sdf1.parse("2018-11-1 00:00:00")
  val dateTo = sdf1.parse("2018-12-31 23:59:59")
  
val mongoClient = new MongoClient();
val db = mongoClient.getDatabase("bitcoin");  
  
  
  
  def main(args: Array[String]): Unit = {
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
    sc.setLogLevel("ERROR")

    
    //popolamento RDD con dati delle transazioni Bitcoin in formato CSV     
//    val rddTxsRaw = sc.textFile(PATH+"large/*.csv")
    val rddTxsRaw = sc.textFile(PATH+"transactions/rob.csv")
    val rddTxsHead = rddTxsRaw.first()
    val rddTinyTxs = rddTxsRaw.filter(row => row != rddTxsHead).filter(line=>line.split(",")(6).length()>0)
      .map(line=>{
        val values = line.split(",")
        //input: _id,fee,h,segwit,size,t,tfs,vol,vsize,weight,wfee,hash
        //output: _id,fee,h,segwit,size,t,tfs,vol,hash,confTime,hourOfDay
        val t = sdf1.parse(values(5)).getTime
        val tfs = sdf1.parse(values(6)).getTime            
        (
            sdf2.format(sdf1.parse(values(6))), 
            (values(0),values(1),values(2),values(3),values(4),t, tfs, values(7),values(11), t-tfs, sdf3.format(sdf1.parse(values(6)))))
    })
    println("total number of transactions: "+rddTinyTxs.count)
    
    rddTinyTxs.first()
    
    
////    TODO ROB drop collection
//    MongoConnector(sc).withDatabaseDo(ReadConfig(sc), db => db.drop())
//    val documents = rddTinyTxs.map(t=>{
//      Document.parse("{t: "+t._2._6.longValue()+",tfs: "+t._2._7.longValue()+"}")
//    })
//    MongoSpark.save(documents)
//    if (true) throw new RuntimeException()
    
//    val rdd = MongoSpark.load(sc)
    val rddTxs = rddTinyTxs.map(t=>{
      //TODO ROB calcolcare annche mempool sizE??
      //mempool txs count      
      val tfs = t._2._7
      
      val aggrIterator = db.getCollection("tx").aggregate(ArrayBuffer[Bson](
                                and(org.mongodb.scala.model.Aggregates.filter(lte("tfs", tfs)),org.mongodb.scala.model.Aggregates.filter(gte("t", tfs))),
                                org.mongodb.scala.model.Aggregates.count("counter")
                                ).asJava).iterator()
    val mempoolCount = Integer.valueOf(aggrIterator.next().get("counter").toString())
//    println(mempoolCount)    
    println(mempoolCount + "@" + new Date().getTime)    
                                                                   
      
      
      (t._1, (t._2._1,t._2._2,t._2._3,t._2._4,t._2._5,t._2._6,t._2._7,t._2._8,t._2._9,t._2._10,t._2._11,mempoolCount))
    })
    
    //caricamento dati pricing Bitcoin e aggregazione per data con calcolo della media prezzo giornaliera 
    //restituisce RDD: timestamp, price
    var rddDailyPriceInit = sc.textFile(PATH+"pricing/coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv")
    val header = rddDailyPriceInit.first()
    val rddDailyPrice = rddDailyPriceInit.filter(row => row != header)
      .map(line=>{
        val values = line.split(",")
        //restituisce Row: data, prezzo cambio USD medio giornaliero
        (sdf2.format(new Date(values(0).toLong*1000)), values(4).toDouble)
        }).filter(!_._2.isNaN()).groupByKey().map(p=>(p._1, p._2.sum/p._2.size))
    println("total number of days in dailyPrice RDD: "+rddDailyPrice.count)    
    
//    val rdd = MongoSpark.load(sc)
    //join per data degli RDDs per aggiungere quotazioni Bitcoin giornaliere
    //resituisce RDD: fee,segwit,size,t,tfs,vol,hash,confTime,hourOfDay,price,mempoolCount
    
    val rddJoined = rddTxs.join(rddDailyPrice)
        .map(t=>{
//val filteredRdd = rdd.filter(doc => doc.getLong("t") > 32).count()
//println(filteredRdd)
          //TODO ROB forse qui posso levare alcuni cast?
            Row(t._2._1._2.toDouble,t._2._1._4.toBoolean,t._2._1._5.toInt,t._2._1._6.toLong,t._2._1._7.toLong,t._2._1._8.toDouble,t._2._1._9.toString(),t._2._1._10.toLong,t._2._1._11.toLong,t._2._2.toDouble,t._2._1._12.toInt)
          })
    
    rddJoined.first()
    

    
      
    //creazione e popolamento dataframe con esclusione righe contenenti campi NaN  
    val df = spark.createDataFrame(rddJoined, BTCSchema.schema).na.drop()
    df.show()
    
      //algoritmo di machine learning supervisionato per predizione tempo conferma Bitcoin mediante regressione lineare
    //definizione vettore di features
//    val assembler = new VectorAssembler().setInputCols(Array("fee","segwit","size","vol","hourOfDay","price")).setOutputCol("features")
    val assembler = new VectorAssembler().setInputCols(Array("fee","size","mempool")).setOutputCol("features")
    val ds = assembler.transform(df).cache()
    
    
    //definizione training e test set
    val Array(training, test) = ds.randomSplit(Array(0.7, 0.3), 123)
    println("training count:"+training.count())
    println("test count:"+test.count())

    
    //creazione modello
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setPredictionCol("predictedConfTime")
      .setLabelCol("confTime")

      
    //apprendimento modello su training set
    val lrModel = lr.fit(training)

    
    //stampa del coefficiente angolare e intercetta della funzione individuata
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    
    //stampa statistiche
    val trainingSummary = lrModel.summary
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")    
    
    
    //applicazione modello su dati di test e valorizzazione predittori
    val predictions = lrModel.transform(test)    
    val extPredictions = predictions      
      .withColumn("~predictedCT", predictions.col("predictedConfTime").cast("Decimal(10,0)"))
      .withColumn("sConfTime", col("confTime")/1000)
      .withColumn("sDiff", abs(col("confTime")-col("predictedConfTime")) / 1000  )
      .withColumn("%Diff", col("sDiff") / col("sConfTime") * 100  )   

      
    extPredictions.show
    println(extPredictions.agg(avg("%Diff")).first.get(0))  
    
    
    
//    //estrazione csv predizioni
//    var lstPreds = new ListBuffer[Array[String]]()
//    lstPreds.append(Array("date", "price", "predictedPrice"))    
//    predictions.sort("date").collect().foreach(r=>lstPreds.append(Array(r.get(0).toString(),decF.format(r.get(1)),decF.format(r.get(7)))))    
//    writeCSV("web/csv/predictions.csv", lstPreds.toList)
    
    //salvataggio modello su file system
    val MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/confTime-ml-model"
    lrModel.write.overwrite().save(MODEL_PATH)    
    println("coefficients from loaded model " + LinearRegressionModel.read.load(MODEL_PATH).coefficients)
    
    //terminazione contesto
    spark.stop()    
  }
  
  
  
  //funzione utilità estrazione csv
  private def writeCSV(fileName: String, values: List[Array[String]]) {
    val outputFile = new BufferedWriter(new FileWriter("./"+fileName))
    val csvWriter = new CSVWriter(outputFile, ',', CSVWriter.NO_QUOTE_CHARACTER)
    csvWriter.writeAll(values)
    outputFile.close()
  }
}