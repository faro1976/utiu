package it.utiu.anavis

import java.io.BufferedWriter
import java.io.FileWriter
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.google.gson.JsonParser
import au.com.bytecode.opencsv.CSVWriter
import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.Actor
import scala.util.Random
import org.apache.spark.sql.types.Metadata
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.LogisticRegressionModel
 

object DummyTrainerActor {

  def makeAsk() = Random.nextBoolean()  
  
  def props(): Props =
    Props(new DummyTrainerActor())

      
  case class StartTraining()
  case class TrainingFinished()  

  //costanti applicative
  val PATH = "hdfs://localhost:9000/dummy.txt"  //HDFS path
  
}

class DummyTrainerActor extends Actor with ActorLogging {
  
    override def receive: Receive = {

        case DummyTrainerActor.StartTraining() =>
          println("entro")
          doTraining()
          
        case DummyTrainerActor.TrainingFinished() =>
          println("restart")
          doTraining()
          
    }
    
    def doTraining() {
import DummyTrainerActor._          
          
    //spark init
    val conf = new SparkConf().setAppName("dummy")
    .setMaster("local")
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    //popolamento RDD con dati delle transazioni Bitcoin in formato json 
    val rdds =spark.read.format("csv").option("header", "true").load(PATH).map(r=>(r.getString(0).toLong,r.getString(1).toLong,r.getString(2).toLong,r.getString(3).toLong,r.getString(4).toLong,r.getString(5).toLong,r.getString(6).toLong,r.getString(7)))
    
    //acquisisco le feature dai siti individuati    
    //converto tutte le feature in un singolo vettore di feature e lo aggiungo come colonna
    val assembler = new VectorAssembler().setInputCols(Array("_1","_2","_3","_4","_5","_6","_7")).setOutputCol("features").setHandleInvalid("keep")
    val dfExtended = assembler.transform(rdds)
    
    
    //  split the dataframe into training and test data
    //generazione casuale del seed per generare dataset differenti ad ogni esecuzione
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = dfExtended.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: " + testData.count())

    
    //aggiunta indicizzazione label
    val labelIndexer = new StringIndexer()
      .setInputCol("_8")
      .setOutputCol("indexedLabel")
      .setHandleInvalid("keep")
      .fit(dfExtended)
    //aggiunta indicizzazione feature
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .setHandleInvalid("keep")
      .fit(dfExtended)
    //aggiunta prediction label      
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

      
    //LOGISTIC REGRESSION CLASSIFIER
    val lr = new LogisticRegression().setMaxIter(5).setRegParam(0.3).setElasticNetParam(0.8)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val pipelineLR = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
    val modelLR = pipelineLR.fit(trainingData)
    val predictionsLR = modelLR.transform(testData)
    predictionsLR.select("predictedLabel", "_8", "features").show(5)
    
    //salvataggio modello su file system
    val MODEL_PATH = "/Users/rob/UniNettuno/dataset/ml-model/dummy-ml-model"
    modelLR.write.overwrite().save(MODEL_PATH)    
    println("coefficients from loaded model " + LogisticRegressionModel.read.load(MODEL_PATH).coefficients)
    
    
    //terminazione contesto
    spark.stop()    
    
    //invio notifica a predictor
    context.actorSelection("/user/BTCConsumer*") ! TrainingFinished()
    self ! TrainingFinished()
  }
  
}