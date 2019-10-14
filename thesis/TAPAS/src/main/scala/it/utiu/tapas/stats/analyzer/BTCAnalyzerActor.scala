package it.utiu.tapas.stats.analyzer

import it.utiu.tapas.base.AbstractAnalyzerActor
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.util.Consts

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.ml.linalg.Vector
import java.text.SimpleDateFormat
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{ DataFrame, SparkSession }
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import akka.actor.Props
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.feature.VectorAssembler
import java.nio.file.StandardOpenOption
import akka.actor.ActorRef

object BTCAnalyzerActor {
  def props(): Props = Props(new BTCAnalyzerActor())

}

class BTCAnalyzerActor() extends AbstractAnalyzerActor(Consts.CS_BTC) {
  override def doInternalAnalysis(spark: SparkSession): (Array[String], scala.collection.immutable.List[Row]) = {
        
    val df1 = spark.read.json(HDFS_CS_PATH + "*")
//        val df1 = spark.read.json(HDFS_CS_PATH + "blockchair/small/*")
    df1.show
    df1.printSchema()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val df2 = df1.select("context.cache.since", "data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")    
    
    //do analytics
    //instant values: difficulty, nodes, mempool_transactions, market_price_usd
    //last 24h values: transactions_24h, volume_24h, average_transaction_fee_24h, inflation_usd_24h

    val dfHourlyWindow = df2
      .groupBy(window(df2.col("since"), "1 hour"))
      .agg(avg("market_price_usd").as("hourly_average_price"))
    dfHourlyWindow.show()
    val df3_1 = df2.withColumn("next_hour", date_trunc("HOUR", col("since") + expr("INTERVAL 1 HOURS")))
    df3_1.show()

    val df3 = df3_1.join(dfHourlyWindow, col("next_hour") === date_trunc("HOUR", col("window.start")))
      .withColumnRenamed("hourly_average_price", "next_avg_price").withColumnRenamed("window", "next_window")
    df3.show()
    
    
    //add yyyy-MM-dd HH date
    val df4 = df3.withColumn("datehour_only", date_format(to_timestamp(col("since"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH"))
    df4.show()
//    //TODO ROB valutare se considerare ultima segnalazione giornaliera vs. media last24h  
//    //instant values, compute average
//    val dfInst = dfAnalyticsPre.groupBy("date_only").agg(mean("difficulty").as("avgDifficulty"), mean("nodes").as("avgNodes"), mean("mempool_transactions").as("avgMempoolTxs"), mean("market_price_usd").as("avgPriceUSD"))
//    //last 24hh values, get the later value of day
//    val dfKeysLast24 = dfAnalyticsPre.groupBy("date_only").agg(max("since").as("since"))
//    dfKeysLast24.show()
//    //extract last24hh values by key of later in day value
//    val dfLast24 = dfAnalyticsPre.join(dfKeysLast24, "since")
//    dfLast24.show()
//    //join average and later in day values
//    val dfJoineddfAnalytics = dfInst.join(dfLast24, "date_only")
    
    //instant and last24h values, compute average
    val df5 = df4.groupBy("datehour_only").agg(mean("difficulty").as("avgDifficulty"), mean("nodes").as("avgNodes"), mean("mempool_transactions").as("avgMempoolTxs"), mean("market_price_usd").as("avgPriceUSD")
        , mean("transactions_24h").as("avgTx24h"), mean("volume_24h").as("avgVolume24h"), mean("average_transaction_fee_24h").as("avgTxFee24h"), mean("inflation_usd_24h").as("avgInflatUSD24h"), mean("next_avg_price").as("avgNextAvgPrice"))
    df5.show()
    val ret = df5.sort("datehour_only")
 
    //compute correlation matrix   
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "volume_24h", "mempool_transactions", "mempool_size", "mempool_tps", "mempool_total_fee_usd", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "average_transaction_fee_usd_24h", "market_price_usd", "next_difficulty_estimate", "suggested_transaction_fee_per_byte_sat")).setOutputCol("features")
      .setHandleInvalid("skip")
    val df6 = assembler.transform(df2)    
    computeCorrelationMatrix(df6)
    
    (ret.columns, ret.collectAsList().asScala.toList)        
  }

  
  private def computeCorrelationMatrix(df: DataFrame) {
    //compute correlation matrix
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(coeff2.toString(Int.MaxValue, Int.MaxValue))

    val buff = new StringBuilder("\n")
    for (v <- coeff2.colIter) {
      for (i <- v.toArray) {
        buff.append(v.toArray.mkString(",") + "\n")
      }
    }
    writeFile(ANALYTICS_OUTPUT_FILE+".corrMtx", buff.toString, None)
  }  
}