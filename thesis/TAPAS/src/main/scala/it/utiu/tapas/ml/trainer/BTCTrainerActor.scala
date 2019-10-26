package it.utiu.anavis

import scala.util.Random

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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.ml.linalg.Vector
import java.text.SimpleDateFormat
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{ DataFrame, SparkSession }
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File
import org.apache.spark.ml.Transformer
import akka.actor.ActorLogging
import akka.event.LoggingAdapter
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.regression.RandomForestRegressor
import java.nio.file.StandardOpenOption
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import it.utiu.tapas.base.AbstractRegressionTrainerActor

object BTCTrainerActor {
  def props(): Props =
    Props(new BTCTrainerActor())
}

class BTCTrainerActor extends AbstractRegressionTrainerActor(Consts.CS_BTC) {
  def printWindow(windowDF: DataFrame, aggCol: String) = {
    windowDF.sort("window.start").
      select("window.start", "window.end", s"$aggCol").
      show(truncate = false)
  }
  override def doInternalTraining(spark: SparkSession): List[(String, Transformer, DataFrame, (Long, Long))] = {
    import org.apache.spark.sql.functions._

    //load dataset from csv inferring schema
    val df1 = spark.read.json(HDFS_CS_PATH + "*")

    import spark.implicits._
    val df2 = df1.select("context.cache.since", "data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")

    val dfHourlyWindow = df2
      .groupBy(window(df2.col("since"), "1 hour"))
      .agg(avg("market_price_usd").as("hourly_average_price"))
    //shift end to next minute adding 60 secs
    //reset minutes and seconds, then shift to next hour
    val df3_1 = df2.withColumn("next_hour", date_trunc("HOUR", col("since") + expr("INTERVAL 1 HOURS")))
    //and at the end join by window start time
    val df3_2 = df3_1.withColumn("prev_hour", date_trunc("HOUR", col("since"))) //.select(col("since"), col("prev_hour"))
    //and at the end join by window start time

    val df3_3 = df3_2.join(dfHourlyWindow, col("next_hour") === date_trunc("HOUR", col("window.start")))
      .withColumnRenamed("hourly_average_price", "next_avg_price").withColumnRenamed("window", "next_window")
      .join(dfHourlyWindow, col("prev_hour") === date_trunc("HOUR", col("window.end")))
      .withColumnRenamed("hourly_average_price", "prev_avg_price").withColumnRenamed("window", "prev_window")
    val df3 = df3_3

    //define model features
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat", /*"prev_avg_price", */ "market_price_usd")).setOutputCol("features")
      .setHandleInvalid("skip")
    //define model label
    val df4 = assembler.transform(df3).withColumn("label", df3.col("next_avg_price")).cache()

    df4.show()
    df4.printSchema()

    //define training and test sets randomly splitted
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = df4.randomSplit(Array(0.7, 0.3), splitSeed)
    val trainCount = trainingData.count()
    val testCount = testData.count()
    println("training count:" + trainCount)
    println("test count:" + testCount)

    //(modelName, model, predictions, (train count, test count))
    val evals = ArrayBuffer[(String, Transformer, DataFrame, (Long, Long))]()

    //LinearRegression
    //build regression model
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //learn from training set
    val modelLR = lr.fit(trainingData)

    //print model parameters
    println(s"Coefficients: ${modelLR.coefficients} Intercept: ${modelLR.intercept}")

    //validate model by test set
    val predictionsLR = modelLR.transform(testData)

    evals.append(("LinearRegression", modelLR, predictionsLR, (trainCount, testCount)))

    //DecisionTreeRegression
    //build regression model
    val dtr = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    //learn from training set
    val modelDTR = dtr.fit(trainingData)

    //validate model by test set
    val predictionsDTR = modelDTR.transform(testData)

    evals.append(("DecisionTreeRegression", modelDTR, predictionsDTR, (trainCount, testCount)))

    //RandomForestRegressor
    //build regression model
    val rfr = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    //learn from training set
    val modelRFR = rfr.fit(trainingData)

    //validate model by test set
    val predictionsRFR = modelRFR.transform(testData)

    evals.append(("RandomForestRegressor", modelRFR, predictionsRFR, (trainCount, testCount)))

    //GBT
    //build regression model
    val GBT = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    //learn from training set
    val modelGBT = GBT.fit(trainingData)

    //print model parameters
    println(s"Num trees: ${modelGBT.numTrees}")

    //validate model by test set
    val predictionsGBT = modelGBT.transform(testData)
    evals.append(("GBTRegressor", modelGBT, predictionsGBT, (trainCount, testCount)))

    //compute correlation matrix
    val assemblerCM = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "volume_24h", "mempool_transactions", "mempool_size", "mempool_tps", "mempool_total_fee_usd", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "average_transaction_fee_usd_24h", "market_price_usd", "next_difficulty_estimate", "suggested_transaction_fee_per_byte_sat")).setOutputCol("features")
      .setHandleInvalid("skip")
    val dfCM = assemblerCM.transform(df2)
    computeCorrelationMatrix(dfCM)

    //write historical prediction results
    val sb = new StringBuffer();
    val locTest = predictionsGBT.sort("since").collect()
    val buff = ArrayBuffer[(Date, Double, Double)]()
    for (r <- locTest) {
      sb.append(r.getAs[String]("since") + "," + r.getAs[Double]("prediction") + "," + r.getAs[Double]("label").toDouble + "\n")
    }
    writeFile(RT_OUTPUT_PATH + Consts.CS_BTC + "-historical.csv", sb.toString(), None)

    evals.toList
  }
}