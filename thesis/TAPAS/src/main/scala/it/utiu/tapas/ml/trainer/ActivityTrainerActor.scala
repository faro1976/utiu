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

object ActivityTrainerActor {
  def props(): Props =
    Props(new ActivityTrainerActor())
}

class ActivityTrainerActor extends AbstractTrainerActor(Consts.CS_ACTIVITY) {

  override def doInternalTraining(spark: SparkSession): Transformer = {
    //load dataset from csv inferring schema from header
    val df1 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(HDFS_CS_PATH + "*").toDF("_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9")
    df1.show

    //define features
    val assembler = new VectorAssembler().setInputCols(Array("_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8")).setOutputCol("features")
    val df2 = assembler.transform(df1);

    //define training and test sets randomly splitted
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: " + testData.count())

    //build logistic regression classifier
    val lr = new LogisticRegression()
      .setLabelCol("_9")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    //      .setFamily("multinomial")

    //learn from training set
    val modelLR = lr.fit(trainingData)
    //validate model by test set
    val predictionsLR = modelLR.transform(testData)
    predictionsLR.select("prediction", "_9", "features").show(10)

    //print ml evaluation
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("_9")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictionsLR)
    println("accuracy: " + accuracy)

    return modelLR
  }

}