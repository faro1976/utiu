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

object WineTrainerActor {
  def props(): Props =
    Props(new WineTrainerActor())
}

class WineTrainerActor extends AbstractTrainerActor(Consts.CS_WINE) {

  override def doInternalTraining(spark: SparkSession): MLWritable = {

    
    //caricamento dataset come CSV inferendo lo schema dall'header
    val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(AbstractBaseActor.HDFS_PATH+Consts.CS_WINE+"/*")

    //definisco le feature e le aggiungo come colonna "features"
    val assembler = new VectorAssembler().setInputCols(Array("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")).setOutputCol("features")
    val df2 = assembler.transform(df1);

    //generazione casuale del seed per generare dataset train/test differenti ad ogni esecuzione
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: " + testData.count())

    //LOGISTIC REGRESSION CLASSIFIER
    val lr = new LogisticRegression()
      .setLabelCol("Class")
      .setFeaturesCol("features")
      .setPredictionCol("predictedClass")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    //      .setFamily("multinomial")
      
    val modelLR = lr.fit(df2)
    val predictionsLR = modelLR.transform(testData)
    predictionsLR.select("predictedClass", "Class", "features").show(200)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Class")
      .setPredictionCol("predictedClass")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictionsLR)
    println("accuracy: " + accuracy)

    return modelLR
  }

}