package it.utiu.anavis

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
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.google.gson.JsonParser
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.types.Metadata



object Runner {
  //costanti applicative
  val PATH = "hdfs://localhost:9000/blockchain/"  //HDFS path
//  val PATH="/Users/rob/tmp/blockchain/"
  val DATE_PATTERN = "yyyyMMdd"
//  val sdfm = new SimpleDateFormat("yyMMddhhmm")
  val decF = new DecimalFormat("#.####");  
  val jsonParser = new JsonParser()
  
  
  
  def main(args: Array[String]): Unit = {
    //spark init
    val conf = new SparkConf().setAppName("Progetto Big Data Analytics and Visualization")
//    .setMaster("local")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    //popolamento RDD con dati delle transazioni Bitcoin in formato json 
    val rddTxs = sc.textFile(PATH + "*.json").map(r => {
      val jsonTx = jsonParser.parse(r).getAsJsonObject
      //popolamento classe Transaction
      new Transaction(jsonTx.get("hash").getAsString, new Date(jsonTx.get("block_timestamp").getAsLong*1000), jsonTx.get("input_count").getAsInt, jsonTx.get("output_count").getAsInt, jsonTx.get("fee").getAsDouble, jsonTx.get("output_value").getAsDouble, jsonTx.get("size").getAsLong)
    }).cache()
    println("total number of transactions: "+rddTxs.count)

    
    //estrazione csv singola giornata per analisi anatomia delle transazioni
    val dateFrom = new Date(117, 11, 17);  //estrazione del 17/12/2017 con picco quotazione più alto Bitcoin 
    val dateTo = new Date(117, 11, 18); 
    var lstAnatomy = new ListBuffer[Array[String]]()
    //header
    lstAnatomy.append(Array("hash", "input_count", "output_count"))    
    rddTxs.filter(t=>t.timestamp.after(dateFrom) && t.timestamp.before(dateTo)).map(t=>(t.hash, t.inputCount, t.outputCount)).collect().foreach(t=>lstAnatomy.append(Array(t._1.toString,t._2.toString(),t._3.toString)))    
    writeCSV("web/csv/txsAnatomy.csv", lstAnatomy.toList)


    //agggregazione transazioni per statistiche giornaliere
    //restituisce RDD: numero transazioni confermate in blockchain, volume transato totale, volume transato medio, fee media
    val rddDailyStats = rddTxs.map(t=>(new SimpleDateFormat(DATE_PATTERN).format(t.timestamp), t)).groupByKey().map(t=>(t._1, (t._2.size.toDouble, t._2.map(t2=>t2.outputValue).sum.toDouble,  t._2.map(t2=>t2.outputValue).sum/t._2.size, t._2.map(t2=>t2.outputValue).sum/t._2.size)))
    println("total number of days in dailyStats RDD: "+rddDailyStats.count)    
    
    
    //caricamento dati pricing Bitcoin e aggregazione per data con calcolo della media prezzo giornaliera 
    //restituisce RDD: timestamp, price
    var rddDailyPriceInit = sc.textFile(PATH+"coinbaseUSD_1-min_data_2014-12-01_to_2019-01-09.csv")
    val header = rddDailyPriceInit.first()
    val rddDailyPrice = rddDailyPriceInit.filter(row => row != header)
      .map(line=>{
        val values = line.split(",")
        //restituisce Row: data, prezzo cambio USD medio giornaliero
        (new SimpleDateFormat(DATE_PATTERN).format(new Date(values(0).toLong*1000)), values(4).toDouble)
        }).filter(!_._2.isNaN()).groupByKey().map(p=>(p._1, p._2.sum/p._2.size))
    println("total number of days in dailyPrice RDD: "+rddDailyPrice.count)    
    
    
    //join per data degli RDDs statistiche transazioni giornaliere e quotazioni Bitcoin giornaliere
    //resituisce RDD: timestamp, price, total_transactions, total_amount, average_amount, average_fee 
    val rddJoined = rddDailyStats.join(rddDailyPrice)
        .map(e=>Row(e._1,e._2._2,e._2._1._1,e._2._1._2,e._2._1._3,e._2._1._4))        
                
    
    //definzione schema dataframe finale    
    val schema = new StructType()
      .add(StructField("date", StringType, false, Metadata.empty))
      .add(StructField("price", DoubleType, false, Metadata.empty))
      .add(StructField("total_transactions", DoubleType, false, Metadata.empty))
      .add(StructField("total_amount", DoubleType, false, Metadata.empty))
      .add(StructField("average_amount", DoubleType, false, Metadata.empty))
      .add(StructField("average_fee", DoubleType, false, Metadata.empty))      
    
      
    //creazione e popolamento dataframe con esclusione righe contenenti campi NaN  
    val df = spark.createDataFrame(rddJoined, schema).na.drop()
    df.show()
    
    
    //creazione CSV statistiche e quotazioni raggruppate per giorno
    var lstStats = new ListBuffer[Array[String]]()
    //header
    lstStats.append(Array("date", "price", "total_transactions", "total_amount", "average_amount", "average_fee"))    
    df.sort("date").collect().foreach(r=>lstStats.append(Array(r.get(0).toString(),decF.format(r.get(1)),r.get(2).toString(),r.get(3).toString(),decF.format(r.get(4)),decF.format(r.get(5)))))    
    writeCSV("web/csv/dailyStats.csv", lstStats.toList)
    
    
    //algoritmo di machine learning supervisionato per predizione quotazione Bitcoin mediante regressione lineare
    //definizione vettore di features
    val assembler = new VectorAssembler().setInputCols(Array("total_transactions","total_amount","average_amount","average_fee")).setOutputCol("features")
    val dfML = assembler.transform(df).cache()
    
    
    //definizione training e test set
    val Array(training, test) = dfML.randomSplit(Array(0.7, 0.3), 123)
    println("training count:"+training.count())
    println("test count:"+test.count())

    
    //creazione modello
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setPredictionCol("predictedPrice")
      .setLabelCol("price")

      
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
    predictions.show()
    var lstPreds = new ListBuffer[Array[String]]()
    
    
    //estrazione csv predizioni
    lstPreds.append(Array("date", "price", "predictedPrice"))    
    predictions.sort("date").collect().foreach(r=>lstPreds.append(Array(r.get(0).toString(),decF.format(r.get(1)),decF.format(r.get(7)))))    
    writeCSV("web/csv/predictions.csv", lstPreds.toList)
    
    
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