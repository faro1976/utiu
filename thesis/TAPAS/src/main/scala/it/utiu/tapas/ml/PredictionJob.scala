package it.utiu.tapas.ml

import java.io.BufferedWriter
import java.io.FileWriter
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.Row


object PredictionJob {
  //costanti applicative
  val PATH = "hdfs://localhost:9000/bitcoin/"  //HDFS path
//  val sdfm = new SimpleDateFormat("yyMMddhhmm")
  val decF = new DecimalFormat("#.####");  
  val sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
  val sdf3 = new SimpleDateFormat("hh")
  
  
  
  def main(args: Array[String]): Unit = {
    //spark init
    val conf = new SparkConf().setAppName("TAPAS - a Timely Analytics & Predictions Actor System")
    .setMaster("local")
    .set("spark.driver.bindAddress", "127.0.0.1")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("DEBUG")

    
    //popolamento RDD con dati delle transazioni Bitcoin in formato CSV     
    val rddTxsRaw = sc.textFile(PATH+"transactions/*.csv")
    val rddTxsHead = rddTxsRaw.first()
    val rddTxs = rddTxsRaw.filter(row => row != rddTxsHead).filter(line=>line.split(",")(6).length()>0)
      .map(line=>{
        val values = line.split(",")
        //input: _id,fee,h,segwit,size,t,tfs,vol,vsize,weight,wfee,hash
        //output: _id,fee,h,segwit,size,t,tfs,vol,hash,confTime,hourOfDay
        val t = sdf1.parse(values(5)).getTime
        val tfs = sdf1.parse(values(6)).getTime    
        (
            sdf2.format(sdf1.parse(values(6))), 
            (values(0),values(1),values(2),values(3),values(4),t, tfs, values(7),values(11), t-tfs, sdf3.format(sdf1.parse(values(6)))))
    }).filter(t=>t._2._7>0)
    println("total number of transactions: "+rddTxs.count)
    
    rddTxs.first()
    
    
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
    
    
    //join per data degli RDDs statistiche transazioni giornaliere e quotazioni Bitcoin giornaliere
    //resituisce RDD: _id,fee,h,segwit,size,t,tfs,vol,hash,confTime,hourOfDay,price 
    val rddJoined = rddTxs.join(rddDailyPrice)
        .map(t=>(t._2._1._1,t._2._1._2,t._2._1._3,t._2._1._4,t._2._1._5,t._2._1._6,t._2._1._7,t._2._1._8,t._2._1._9,t._2._1._10,t._2._1._11,t._2._2))
    
    rddJoined.first()
//                
//    
//    //definzione schema dataframe finale    
//    val schema = new StructType()
//      .add(StructField("date", StringType, false, Metadata.empty))
//      .add(StructField("price", DoubleType, false, Metadata.empty))
//      .add(StructField("total_transactions", DoubleType, false, Metadata.empty))
//      .add(StructField("total_amount", DoubleType, false, Metadata.empty))
//      .add(StructField("average_amount", DoubleType, false, Metadata.empty))
//      .add(StructField("average_fee", DoubleType, false, Metadata.empty))      
//    
//      
//    //creazione e popolamento dataframe con esclusione righe contenenti campi NaN  
//    val df = spark.createDataFrame(rddJoined, schema).na.drop()
//    df.show()
//    
//    
//    //creazione CSV statistiche e quotazioni raggruppate per giorno
//    var lstStats = new ListBuffer[Array[String]]()
//    //header
//    lstStats.append(Array("date", "price", "total_transactions", "total_amount", "average_amount", "average_fee"))    
//    df.sort("date").collect().foreach(r=>lstStats.append(Array(r.get(0).toString(),decF.format(r.get(1)),r.get(2).toString(),r.get(3).toString(),decF.format(r.get(4)),decF.format(r.get(5)))))    
//    writeCSV("web/csv/dailyStats.csv", lstStats.toList)
//    
//    
//    //algoritmo di machine learning supervisionato per predizione quotazione Bitcoin mediante regressione lineare
//    //definizione vettore di features
//    val assembler = new VectorAssembler().setInputCols(Array("total_transactions","total_amount","average_amount","average_fee")).setOutputCol("features")
//    val dfML = assembler.transform(df).cache()
//    
//    
//    //definizione training e test set
//    val Array(training, test) = dfML.randomSplit(Array(0.7, 0.3), 123)
//    println("training count:"+training.count())
//    println("test count:"+test.count())
//
//    
//    //creazione modello
//    val lr = new LinearRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
//      .setFeaturesCol("features")
//      .setPredictionCol("predictedPrice")
//      .setLabelCol("price")
//
//      
//    //apprendimento modello su training set
//    val lrModel = lr.fit(training)
//
//    
//    //stampa del coefficiente angolare e intercetta della funzione individuata
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
//
//    
//    //stampa statistiche
//    val trainingSummary = lrModel.summary
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")    
//    
//    
//    //applicazione modello su dati di test e valorizzazione predittori
//    val predictions = lrModel.transform(test)
//    predictions.show()
//    var lstPreds = new ListBuffer[Array[String]]()
//    
//    
//    //estrazione csv predizioni
//    lstPreds.append(Array("date", "price", "predictedPrice"))    
//    predictions.sort("date").collect().foreach(r=>lstPreds.append(Array(r.get(0).toString(),decF.format(r.get(1)),decF.format(r.get(7)))))    
//    writeCSV("web/csv/predictions.csv", lstPreds.toList)
    
    
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