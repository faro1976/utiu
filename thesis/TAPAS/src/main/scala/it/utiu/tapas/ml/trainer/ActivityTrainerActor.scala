package it.utiu.anavis

import scala.util.Random

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession

import akka.actor.Props
import it.utiu.tapas.base.AbstractBaseActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.util.Consts
import org.apache.spark.ml.Transformer
import java.util.Date
import java.nio.file.StandardOpenOption
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import scala.collection.mutable.ArrayBuffer


object ActivityTrainerActor {
  def props(): Props =
    Props(new ActivityTrainerActor())
}

class ActivityTrainerActor extends AbstractTrainerActor(Consts.CS_ACTIVITY) {

  override def doInternalTraining(spark: SparkSession): Transformer = {
    import org.apache.spark.sql.functions._
    
    //load dataset from csv inferring schema from header
    val df1 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(HDFS_CS_PATH + "*").toDF("_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9").withColumn("label", col("_9"))
    df1.show

    //define features
    val assembler = new VectorAssembler().setInputCols(Array("_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8")).setOutputCol("features")
    val df2 = assembler.transform(df1);

    //define training and test sets randomly splitted
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    val trainCount = trainingData.count()
    val testCount = testData.count()
    println("training count:" + trainCount)
    println("test count:" + testCount)

    val evals = ArrayBuffer[(Transformer, Double)]()
    
    //LOGISTIC REGRESSION CLASSIFIER
    val lr = new LogisticRegression().setMaxIter(3).setRegParam(0.3).setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")
    val modelLR = lr.fit(trainingData)
    val predictionsLR = modelLR.transform(testData)
    val evalLR = calculateMetrics("LogisticRegression", predictionsLR, (trainCount, testCount))
    evals.append((modelLR, evalLR))

    //DECISION TREES CLASSIFIER
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    val modelDT = dt.fit(trainingData)
    val predictionsDT = modelDT.transform(testData)
    val evalDT = calculateMetrics("DecisionTreeClassifier", predictionsDT, (trainCount, testCount))
    evals.append((modelDT, evalDT))

    //RANDOM FOREST CLASSIFIER
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    val modelRF = rf.fit(trainingData)
    val predictionsRF = modelRF.transform(testData)
    val evalRF = calculateMetrics("RandomForestClassifier", predictionsRF, (trainCount, testCount))
    evals.append((modelRF, evalRF))    
    
    //return best fit model by accuracy evaluator
    evals.maxBy(_._2)._1
  }

  
  //funzione generica per calcolo indicatori dei predittori
  def calculateMetrics(algo: String, predictions: DataFrame, rows: (Long, Long)): Double = {
    import predictions.sparkSession.implicits._
    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    
    val str = tmstFormat.format(new Date()) + "," + algo + "," + (accuracy +","+counttotal+","+correct+","+wrong+","+trueP+","+falseP+","+trueN+","+falseN+","+ratioWrong+","+ratioCorrect) + "," + rows._1 + "," + rows._2 + "\n"
    writeFile(RT_OUTPUT_PATH + Consts.CS_BTC + "-classification-eval.csv", str, Some(StandardOpenOption.APPEND))
    accuracy
  }
  
}