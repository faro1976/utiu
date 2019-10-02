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
import org.apache.spark.sql.{ DataFrame, SparkSession }
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File

object BTCTrainerActor {
  def props(): Props =
    Props(new BTCTrainerActor())
}

class BTCTrainerActor extends AbstractTrainerActor(Consts.CS_BTC) {
  def printWindow(windowDF: DataFrame, aggCol: String) = {
    windowDF.sort("window.start").
      select("window.start", "window.end", s"$aggCol").
      show(truncate = false)
  }
  override def doInternalTraining(spark: SparkSession): MLWritable = {
    import org.apache.spark.sql.functions._

    //load dataset from csv inferring schema
    val df1 = spark.read.json(HDFS_CS_PATH + "blockchair/*")
//        val df1 = spark.read.json(HDFS_CS_PATH + "blockchair/small/*")
    df1.show
    df1.printSchema()
    import spark.implicits._
    val df2 = df1.select("context.cache.since", "data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")

    val dfHourlyWindow = df2
      .groupBy(window(df2.col("since"), "1 hour"))
      .agg(avg("market_price_usd").as("hourly_average_price"))
    dfHourlyWindow.show()
    //shift end to next minute adding 60 secs
    //    val df3 = df2.withColumn("end", date_format(to_date(timeUdf(col("since")), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:00:00"))
    //reset minutes and seconds, then shift to next hour
    //    val df3 = df2.withColumn("end", (col("since")-minute(col("since"))-second(col("since"))+expr("INTERVAL 1 HOURS")))
    //    val df3_1 = df2.withColumn("next_hour", date_trunc("HOUR", col("since")+expr("INTERVAL 1 HOURS"))).select(col("since"), col("next_hour"), col("hourly_average_price").as("next_avg_price"))
    val df3_1 = df2.withColumn("next_hour", date_trunc("HOUR", col("since") + expr("INTERVAL 1 HOURS")))
    //and at the end join by window start time
    //      .join(dfHourlyWindow, col("next_hour") === date_trunc("HOUR",col("window.start")))
    df3_1.show()
    //    val df3_2 = df2.withColumn("prev_hour", date_trunc("HOUR", col("since"))).select(col("since"), col("prev_hour"), col("hourly_average_price").as("prev_avg_price"))
    val df3_2 = df3_1.withColumn("prev_hour", date_trunc("HOUR", col("since"))) //.select(col("since"), col("prev_hour"))
    //and at the end join by window start time
    //      .join(dfHourlyWindow, col("prev_hour") === date_trunc("HOUR",col("window.end")))
    //    val df3 = df2.join(df3_1, "since").join(df3_2, "since")
    df3_2.show()

    val df3_3 = df3_2.join(dfHourlyWindow, col("next_hour") === date_trunc("HOUR", col("window.start")))
      .withColumnRenamed("hourly_average_price", "next_avg_price").withColumnRenamed("window", "next_window")
      .join(dfHourlyWindow, col("prev_hour") === date_trunc("HOUR", col("window.end")))
      .withColumnRenamed("hourly_average_price", "prev_avg_price").withColumnRenamed("window", "prev_window")
    val df3 = df3_3
    df3.show()

    //define model features
//    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat", "prev_avg_price", "next_avg_price")).setOutputCol("features")
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat", "prev_avg_price", "market_price_usd")).setOutputCol("features")
      .setHandleInvalid("skip")
    //define model label
//    val df4 = assembler.transform(df3).withColumn("label", df3.col("market_price_usd")).cache()
      val df4 = assembler.transform(df3).withColumn("label", df3.col("next_avg_price")).cache()
      
    df4.show()
    df4.printSchema()

    //define training and test sets randomly splitted
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = df4.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training count:" + trainingData.count())
    println("test count:" + testData.count())

    //build GBT regression model
    val GBT = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    //learn from training set
    val modelGBT = GBT.fit(trainingData)

    //print model parameters
    println(s"Num trees: ${modelGBT.numTrees}")

    //validate model by test set
    val predictions = modelGBT.transform(testData)
    predictions.show()

    //get predictions
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

    //print ml metrics
    evaluator.setMetricName("mse")
    val mse = evaluator.evaluate(predictions)
    println(s"Mean squared error: $mse")

    evaluator.setMetricName("r2")
    val r2 = evaluator.evaluate(predictions)
    println(s"r2: $r2")

    evaluator.setMetricName("mae")
    val mae = evaluator.evaluate(predictions)
    println(s"Mean absolute error: $mae")

    //do analytics
    //instant values: difficulty, nodes, mempool_transactions, market_price_usd
    //last 24h values: transactions_24h, volume_24h, average_transaction_fee_24h, inflation_usd_24h

    //add yyyy-MM-dd date
    val dfAnalyticsPre = df4.withColumn("date_only", date_format(to_date(col("since"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
    dfAnalyticsPre.show()
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
    val dfAnalyticsRes = dfAnalyticsPre.groupBy("date_only").agg(mean("difficulty").as("avgDifficulty"), mean("nodes").as("avgNodes"), mean("mempool_transactions").as("avgMempoolTxs"), mean("market_price_usd").as("avgPriceUSD")
        , mean("transactions_24h").as("avgTx24h"), mean("volume_24h").as("avgVolume24h"), mean("average_transaction_fee_24h").as("avgTxFee24h"), mean("inflation_usd_24h").as("avgInflatUSD24h"))
    dfAnalyticsRes.show()
    if (Files.exists(Paths.get(ANALYTICS_OUTPUT_FILE))) new File(ANALYTICS_OUTPUT_FILE).delete() 
        
    dfAnalyticsRes.sort("date_only").write.csv(ANALYTICS_OUTPUT_FILE)

        
    //compute correlation matrix
    val Row(coeff1: Matrix) = Correlation.corr(df4, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df4, "features", "spearman").head
    println(coeff2.toString(Int.MaxValue, Int.MaxValue))

    for (v <- coeff2.colIter) {
      for (i <- v.toArray) {
        println(i)
      }
    }
    
    return modelGBT
  }

}