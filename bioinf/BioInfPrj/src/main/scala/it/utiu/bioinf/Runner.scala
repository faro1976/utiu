package it.utiu.bioinf

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.util.control.Breaks._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.GBTClassificationModel
import scala.util.Random
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import shapeless.the
import org.spark_project.dmg.pmml.ROC
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.SparkConf



object Runner {
  //costanti applicative
  var PATH = "hdfs://localhost:9000/bioinf/"  //HDFS path
  var ITER_NUM = 3  //numero iterazioni algo ml
 
  
  
  def main(args: Array[String]): Unit = {
    //spark init
    val conf = new SparkConf().setAppName("Progetto Big Data applicati alla Bioinformatica")    
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    
    //defnizione geni interessati
    val clinics = Array("manually_curated__tissue_status", "biospecimen__shared__patient_id")
    val genesBRCA = Array("A2M", "ABCB1", "ABCC1", "ACOT7", "ACOX2", "ADAMTS16", "ADAMTS17", "ADORA2A", "AQP1", "CA12", "CDCP1", "CREB3L1", "CRYAB", "FGF1", "IL11RA", "INHBA", "ITIH5", "KIF26B", "LRRC3B", "MEG3", "MUC1", "PRKD1", "SDPR")
    println("GENES BRCA length: "+ genesBRCA.length)
    
    //popolamento dinamico dei siti distinti (CpG) da mappare come feature in funzione del gene di appartenenza - recupero da file annotations/humanMethylation450_annotations.bed 
    val rddSites = sc.textFile(PATH + "annotations/humanMethylation450_annotations.bed").map(r => {
      val vals = r.split("\t")
      var ret = ""
      if (genesBRCA.contains(vals(5))) {
        ret = vals(4).replace(".", "_")
      }
      ret
    }).filter(_ != "").distinct()
    val sitesBRCA = rddSites.collect()
    println("SITES BRCA length: " + sitesBRCA.length)

    
    //funzione per parsing riga file metadati clinici biospecimen (.meta)
    def parseMeta(t: (String, String)): Row = {
      val metaKV = Map[String, String]()
      val lines = t._2.split("\n")
      lines.map(line => {
        val kv = line.split("\t")
        //key -> value
        metaKV += (kv(0) -> kv(1))
      })
      var status: Double = Double.NaN
      //label tumoral/normal in funzione dello stato 
      metaKV.get("manually_curated__tissue_status").get match {
        case "tumoral" => status = 1.0
        case "normal"  => status = 0.0
        case _         => throw new RuntimeException("tissue status not recognized")
      }
      //restituzione row del dataframe metadati
      Row(metaKV.get("manually_curated__opengdc_id").get, metaKV.get("biospecimen__shared__patient_id").get, metaKV.get("manually_curated__tissue_status").get, status)
    }

    //definizione schema dataframe metadati
    val schemaSamplePatient = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      .add(StructField("biospecimen__shared__patient_id", StringType, false))
      .add(StructField("manually_curated__tissue_status", StringType, false))
      .add(StructField("label", DoubleType, false))

    //lettura metadati  (con lettura autocontenuta mediante wholeTextFiles)
    val rddMeta = sc.wholeTextFiles(PATH + "*.meta").map(parseMeta)
    val dfMeta = spark.createDataFrame(rddMeta, schemaSamplePatient).cache
    println("META DATAFRAME - total metadata items: " + dfMeta.count())
    println("META DATAFRAME - total patients: " + dfMeta.select("biospecimen__shared__patient_id").distinct().count())
    dfMeta.show

    
    //funzione per parsing riga file analisi metilazione (.bed)    
    def parseSample(t: (String, String)): Row = {
      val CpGKV = Map[String, Double]()
      val lines = t._2.split("\n")
      lines.foreach(line => {
        val params = line.split("\t")
        val site = params(4).replace(".", "_")
        //verifica sito se di interesse
        if (sitesBRCA.contains(site)) {
          CpGKV += (site -> params(5).toDouble)
        }
      })
      val filename = t._1.split("/")
      val strs = List[String](filename(filename.size - 1).split("\\.")(0))
      val values = ListBuffer[Double]()
      //lista ordinata dei valori beta per i rispettivi CpG come da censimento iniziale
      sitesBRCA.foreach(s => values.append(CpGKV.getOrElse(s, Double.NaN)))
      //restituzione row del dataframe analisi metilazione
      Row.fromSeq((strs ::: values.toList).toSeq)
    }

    //creazione dinamica schema dataframe analisi metilazione, con una colonna per ogni CpG interessato
    val arrObserv = ArrayBuffer[StructField]()
    arrObserv.append(StructField("manually_curated__opengdc_id", StringType, false))
    sitesBRCA.foreach(s => arrObserv.append(StructField(s, DoubleType, false)))
    val schemaSample = new StructType(arrObserv.toArray)

    //lettura file analisi metilazione (con lettura autocontenuta mediante wholeTextFiles)
    val rddSampleF = sc.wholeTextFiles(PATH + "*.bed")
    println("sample files to read : " + rddSampleF.count)
    val rddSample = rddSampleF.map(parseSample)
    val dfSample = spark.createDataFrame(rddSample, schemaSample)

    val dfJoined = dfMeta.join(dfSample, "manually_curated__opengdc_id")
    println("JOINED DATAFRAME")

    
    //acquisisco le feature dai siti individuati    
    //converto tutte le feature in un singolo vettore di feature e lo aggiungo come colonna
    val assembler = new VectorAssembler().setInputCols(sitesBRCA).setOutputCol("features").setHandleInvalid("keep")
    val dfExtended = assembler.transform(dfJoined).cache
    
    
    //  split the dataframe into training and test data
    //generazione casuale del seed per generare dataset differenti ad ogni esecuzione
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = dfExtended.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: " + testData.count())

    
    //aggiunta indicizzazione label
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
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

      
    //SPARK MLlib - dalla documentazione ufficiale
    //Classification Models in MLlib: Spark has several models available for performing binary and multiclass classification out of the box.
    //The following models are available for classification in Spark:
    //-Logistic regression
    //-Decision trees (So under the hood, Apache Spark calls the random forest with one tree.)
    //-Random forests

    //LOGISTIC REGRESSION CLASSIFIER
    val lr = new LogisticRegression().setMaxIter(ITER_NUM).setRegParam(0.3).setElasticNetParam(0.8)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val pipelineLR = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
    val modelLR = pipelineLR.fit(trainingData)
    val predictionsLR = modelLR.transform(testData)
    predictionsLR.select("predictedLabel", "label", "features").show(5)

    //DECISION TREES CLASSIFIER
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val pipelineDT = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    val modelDT = pipelineDT.fit(trainingData)
    val predictionsDT = modelDT.transform(testData)
    predictionsDT.select("predictedLabel", "label", "features").show(5)
    val treeModel = modelDT.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    //RANDOM FOREST CLASSIFIER
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)
    val pipelineRF = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    val modelRF = pipelineRF.fit(trainingData)
    val predictionsRF = modelRF.transform(testData)
    predictionsRF.select("predictedLabel", "label", "features").show(5)
    val rfModel = modelRF.stages(2).asInstanceOf[RandomForestClassificationModel]

    
    //definizione schema dataframe per comparazione risultati algo ML     
    val schemaAlgoSummary = new StructType()
      .add(StructField("algo", StringType, false))
      .add(StructField("count", LongType, false))
      .add(StructField("correct", LongType, false))
      .add(StructField("wrong", LongType, false))
      .add(StructField("ratioCorrect", DoubleType, false))
      .add(StructField("ratioWrong", DoubleType, false))
      .add(StructField("accuracy", DoubleType, false))
      .add(StructField("true positive", LongType, false))
      .add(StructField("false positive", LongType, false))
      .add(StructField("true negative", LongType, false))
      .add(StructField("false negative", LongType, false))   
    //popolamento dataframe comparativo risultati
    val resAlgoSummary = Seq(calculateMetrics("LOGISTIC REGRESSION", predictionsLR), calculateMetrics("DECISION TREE", predictionsDT), calculateMetrics("RANDOM FOREST", predictionsRF)/*, calculateMetrics("GRADIENT-BOOSTED TREE", predictionsGBT)*/)
    val dfAlgoSummary = spark.createDataFrame(spark.sparkContext.parallelize(resAlgoSummary), schemaAlgoSummary)
    dfAlgoSummary.show
    
    spark.stop()
  }

  
  
  //funzione generica per calcolo indicatori dei predittori
  def calculateMetrics(algo: String, dfPrediction: DataFrame): Row = {
    println(s"algo $algo")
    import dfPrediction.sparkSession.implicits._
    val lp = dfPrediction.select("label", "predictedLabel")
    val counttotal = dfPrediction.count()
    val correct = lp.filter($"label" === $"predictedLabel").count()
    val wrong = lp.filter(not($"label" === $"predictedLabel")).count()
    val trueP = lp.filter($"predictedLabel" === 1.0).filter($"label" === $"predictedLabel").count()
    val falseP = lp.filter($"predictedLabel" === 1.0).filter(not($"label" === $"predictedLabel")).count()
    val trueN = lp.filter($"predictedLabel" === 0.0).filter($"label" === $"predictedLabel").count()
    val falseN = lp.filter($"predictedLabel" === 0.0).filter(not($"label" === $"predictedLabel")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    println(s"total:$counttotal, correct:$correct, wrong:$wrong, truP:$trueP, falseP:$falseP, trueN:$trueN, falseN:$falseN, ratioWrong:$ratioWrong, ratioCorrect:$ratioCorrect")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(dfPrediction)
    println(s"$algo accuracy: " + accuracy)
    println(s"Test Error = ${(1.0 - accuracy)}")

    Row(algo, counttotal, correct, wrong, ratioCorrect, ratioWrong, accuracy, trueP, falseP, trueN, falseN)
  }
}
