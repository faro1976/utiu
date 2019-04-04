package it.utiu.anavis

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.google.gson.JsonParser
import java.util.Date

object Runner {
  //costanti applicative
//  var PATH = "hdfs://localhost:9000/bioinf/"  //HDFS path
  var PATH = "/Users/rob/tmp/blockchain2017/"
  
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
      new Transaction(jsonTx.get("hash").getAsString, new Date(jsonTx.get("block_timestamp").getAsLong*1000), jsonTx.get("input_count").getAsInt, jsonTx.get("output_count").getAsInt, jsonTx.get("fee").getAsLong)
    })
    
//    rddTxs.collect().foreach(println)
    println(rddTxs.count)

    //numero transazioni confermate in blockchain per giorno
    rddTxs.map()
    
    spark.stop()
    
  }
}