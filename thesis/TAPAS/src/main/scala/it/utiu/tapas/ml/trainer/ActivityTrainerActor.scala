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
import it.utiu.tapas.base.AbstractClassificationTrainerActor


object ActivityTrainerActor {
  def props(): Props =
    Props(new ActivityTrainerActor())
}

class ActivityTrainerActor extends AbstractClassificationTrainerActor(Consts.CS_ACTIVITY) {

  override def doInternalTraining(spark: SparkSession): List[(String, Transformer, DataFrame, (Long, Long))] = {
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

    //(modelName, model, predictions, (train count, test count))
    val evals = ArrayBuffer[(String, Transformer, DataFrame, (Long, Long))]()
    
    //LOGISTIC REGRESSION CLASSIFIER
    val lr = new LogisticRegression().setMaxIter(3).setRegParam(0.3).setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setFamily("multinomial")      
    val modelLR = lr.fit(trainingData)
    val predictionsLR = modelLR.transform(testData)
    evals.append(("LogisticRegression", modelLR, predictionsLR, (trainCount, testCount)))    

    //DECISION TREES CLASSIFIER
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    val modelDT = dt.fit(trainingData)
    val predictionsDT = modelDT.transform(testData)
    evals.append(("DecisionTreeClassifier", modelDT, predictionsDT, (trainCount, testCount)))
    
    //RANDOM FOREST CLASSIFIER
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    val modelRF = rf.fit(trainingData)
    val predictionsRF = modelRF.transform(testData)
    evals.append(("RandomForestClassifier", modelRF, predictionsRF, (trainCount, testCount)))

    //build confusion matrix for each classifier
    computeConfusionMatrix(predictionsLR, modelLR)
    computeConfusionMatrix(predictionsDT, modelDT)
    computeConfusionMatrix(predictionsRF, modelRF)
    
    evals.toList
  }
}