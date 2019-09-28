package it.utiu.anavis

import scala.util.Random

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{ Matrix, Vectors }
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

import akka.actor.Props
import it.utiu.tapas.base.AbstractBaseActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.util.Consts
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.util.MLUtils
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.ml.linalg.Vector
import java.text.SimpleDateFormat
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{DataFrame, SparkSession}

object BTCTrainerActor {
  def props(): Props =
    Props(new BTCTrainerActor())
}

class BTCTrainerActor extends AbstractTrainerActor(Consts.CS_BTC) {
  def printWindow(windowDF:DataFrame, aggCol:String) ={
      windowDF.sort("window.start").
      select("window.start","window.end",s"$aggCol").
      show(truncate = false)
   }
  override def doInternalTraining(spark: SparkSession): MLWritable = {
    import org.apache.spark.sql.functions._
    
    //caricamento dataset come CSV inferendo lo schema dall'header
    val df1 = spark.read.json(HDFS_CS_PATH + "blockchair/*")
    df1.show
    df1.printSchema()
    import spark.implicits._
    val df2 = df1.select("context.cache.since","data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")
    
    val dfHourlyWindow = df2
      .groupBy(window(df2.col("since"),"1 hour"))
      .agg(avg("market_price_usd").as("hourly_average_price"))      
    dfHourlyWindow.show()
    //shift end to next minute adding 60 secs
    val timeUdf = udf{(time: java.sql.Timestamp) => new java.sql.Timestamp(time.getTime + 60*1000)}
    val df3 = df2.withColumn("end", date_format(to_date(timeUdf(col("since")), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:00:00"))
      .join(dfHourlyWindow, col("end") === col("window.end"))
      
      
    //CORRELATION MATRIX
    //    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "volume_24h", "mempool_transactions", "mempool_size", "mempool_tps", "mempool_total_fee_usd", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "average_transaction_fee_usd_24h", "market_price_usd", "next_difficulty_estimate", "suggested_transaction_fee_per_byte_sat")).setOutputCol("features").setHandleInvalid("keep")
    //    val df4 = assembler.transform(df3)
    //
    //    //    val Row(coeff1: Matrix) = Correlation.corr(df4, "features").head
    //    //println("Pearson correlation matrix:\n" + coeff1.toString)
    //    //
    //    //val Row(coeff2: Matrix) = Correlation.corr(df4, "features", "spearman").head
    //    //println("Spearman correlation matrix:\n" + coeff2.toString)
    //
    //    val Row(coeff1: Matrix) = Correlation.corr(df4, "features").head
    //    println(s"Pearson correlation matrix:\n $coeff1")
    //
    //    val Row(coeff2: Matrix) = Correlation.corr(df4, "features", "spearman").head
    //    println(coeff2.toString(Int.MaxValue, Int.MaxValue))
    //
    //    for (v <- coeff2.colIter) {
    //      for (i <- v.toArray) {
    //        println(i)
    //      }
    //    }

    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat")).setOutputCol("features")
      .setHandleInvalid("skip")
    val df4 = assembler.transform(df3).withColumn("label", df3.col("market_price_usd"))

    //definizione training e test set
    val Array(training, test) = df4.randomSplit(Array(0.7, 0.3), 123)
    println("training count:" + training.count())
    println("test count:" + test.count())
    df4.show(false)
    df4.printSchema()

    //creazione modello
    val GBT = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    //apprendimento modello su training set
    val modelGBT = GBT.fit(training)

//    //stampa del coefficiente angolare e intercetta della funzione individuata
//    println(s"Num trees: ${modelGBT.numTrees}")

    //applicazione modello su dati di test e valorizzazione predittori
    val predictions = modelGBT.transform(test)
    predictions.show()

    // Get predictions
    val valuesAndPreds = df1.rdd.map { point =>
      val prediction = modelGBT.predict(point.getAs[Vector]("features"))
      (prediction, point.getAs[Double]("label"))
    }
    
    //print ml evaluation
     val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")      
    val rmse = evaluator.evaluate(predictions)
    println(s"Root mean squared error: $rmse")    

    evaluator.setMetricName("mse")
    val mse = evaluator.evaluate(predictions)
    println(s"Mean squared error: $mse")    

    evaluator.setMetricName("r2")
    val r2 = evaluator.evaluate(predictions)
    println(s"r2: $r2")
    
    evaluator.setMetricName("mae")
    val mae = evaluator.evaluate(predictions)
    println(s"Mean absolute error: $mae")    
    
    //analytics
    //difficulty, nodes, mempool_transactions, market_price_usd
    //transactions_24h, volume_24h, average_transaction_fee_24h, inflation_usd_24h    
    //group by date and average
    
//    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
    //add yyyy-MM-dd date 
    val dfAnalyticsPre = df3.withColumn("date_only", date_format(to_date(col("since"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
    dfAnalyticsPre.show()
    //instant values, compute average
    val dfInst = dfAnalyticsPre.groupBy("date_only").agg(mean("difficulty").as("avgDifficulty"), mean("nodes").as("avgNodes"), mean("mempool_transactions").as("avgMempoolTxs"), mean("market_price_usd").as("avgPriceUSD"))
    //last 24hh values, get the later value of day
    val dfKeysLast24 = dfAnalyticsPre.groupBy("date_only").agg(max("since").as("since"))
    dfKeysLast24.show()
    //extract last24hh values by key of later in day value
    val dfLast24 = dfAnalyticsPre.join(dfKeysLast24, "since")
    dfLast24.show()
    //join average and later in day values
    val dfJoineddfAnalytics =  dfInst.join(dfLast24, "date_only")
    dfJoineddfAnalytics.show()
    
    dfJoineddfAnalytics.sort("date_only").write.csv(ANALYTICS_OUTPUT_FILE)
    
    return modelGBT
  }

}