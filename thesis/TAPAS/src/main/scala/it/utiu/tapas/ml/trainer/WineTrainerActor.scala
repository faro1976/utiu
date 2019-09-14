package it.utiu.anavis

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props

object WineTrainerActor {

  def makeAsk() = Random.nextBoolean()

  def props(): Props =
    Props(new WineTrainerActor())

  case class StartTraining()
  case class TrainingFinished()

  //costanti applicative
  val PATH = "hdfs://localhost:9000/wine/wine.data" //HDFS path
  val MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/wine-ml-model"
  val SPARK_URL = "spark://localhost:7077"

}

class WineTrainerActor extends Actor with ActorLogging {

  override def receive: Receive = {

    case WineTrainerActor.StartTraining() =>
      println("entro")
      doTraining()

    case WineTrainerActor.TrainingFinished() =>
      println("restart")
      doTraining()

  }

  def doTraining() {
    import WineTrainerActor._

    //spark init
    val conf = new SparkConf()
      .setAppName("wine")
      .setMaster(SPARK_URL)
      .set("spark.executor.instances", "1")
      .set("spark.executor.cores", "1")
      .set("spark.executor.memory", "1g")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //caricamento dataset come CSV inferendo lo schema dall'header
    val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(PATH)

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

    //salvataggio modello su file system    
    modelLR.write.overwrite().save(MODEL_PATH)
    println("features from loaded model " + LogisticRegressionModel.read.load(MODEL_PATH).numFeatures)

    //terminazione contesto
    //TODO ROB lasciare aperto così lo reucpero al prossimo giro??
    //spark.stop()

    //invio notifica a predictor
    context.actorSelection("/user/WineConsumer*") ! TrainingFinished()
    self ! TrainingFinished()
  }

}