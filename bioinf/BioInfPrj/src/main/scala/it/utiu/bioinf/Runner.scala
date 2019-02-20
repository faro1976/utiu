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

object Runner {
  //  val PATH = "/Users/robertofavaroni/UniNettuno/dataset/tcga-brca/methylation_beta_value/"
  val PATH = "/Users/robertofavaroni/UniNettuno/dataset/tcga-brca/sample/"
  val T = "BRCA"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Progetto Big Data applicati alla Bioinformatica")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //            id,status,cg13869341,cg14008030,cg06937459,cg16355945,
    //            00a173ee-32df-4747-8b04-05b3913e3118-mbv,tumoral,0.898509278030815,0.75680426983933,0.978631965130895,0.0928966658174382

    val clinics = Array("manually_curated__tissue_status", "biospecimen__shared__patient_id")
    val genesBRCA = Array("A2M", "ABCB1", "ABCC1", "ACOT7", "ACOX2", "ADAMTS16", "ADAMTS17", "ADORA2A", "AQP1", "CA12", "CDCP1", "CREB3L1", "CRYAB", "FGF1", "IL11RA", "INHBA", "ITIH5", "KIF26B", "LRRC3B", "MEG3", "MUC1", "PRKD1", "SDPR")
    //    val sitesBRCA = Array("cg12417807","cg11139127","cg27166707")

    def parseMeta(t: (String, String)): Row = {
      val metaKV = Map[String, String]()
      val lines = t._2.split("\n")
      lines.map(line => {
        val kv = line.split("\t")
        metaKV += (kv(0) -> kv(1))
      })
      var status: Double = Double.NaN
      metaKV.get("manually_curated__tissue_status").get match {
        case "tumoral" => status = 1.0
        case "normal"  => status = 0.0
        case _         => throw new RuntimeException("tissue status not recognized")
      }
      val r = Row(metaKV.get("manually_curated__opengdc_id").get, metaKV.get("biospecimen__shared__patient_id").get, metaKV.get("manually_curated__tissue_status").get, status)
      r
    }

    val schemaPatient = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      .add(StructField("biospecimen__shared__patient_id", StringType, false))
      .add(StructField("manually_curated__tissue_status", StringType, false))
      .add(StructField("label", DoubleType, false))

    //uso wholeTextFiles per poter leggere i dati in maniera autocontenuta ad ogni iterazione di map
    val rddMeta = sc.wholeTextFiles(PATH + "*.meta").map(parseMeta)
    val dfMeta = spark.createDataFrame(rddMeta, schemaPatient).cache
    println("META DATAFRAME - total metadata items: " + dfMeta.count())
    println("META DATAFRAME - total ptatiens: " + dfMeta.select("biospecimen__shared__patient_id").distinct().count())
    dfMeta.show

    //TODO ROB: un modo più elegante e semplice per sapere associazione siti->gene??
    val rddSites = sc.textFile(PATH + "*.bed").map(r => {
      val vals = r.split("\t")
      var ret = ""
      if (genesBRCA.contains(vals(6))) {
        ret = vals(4).replace(".", "_")
      }
      ret
    }).filter(_ != "").distinct()
    //TODO ROB: fare dimensionality (features) reduction!
    val sitesBRCA = rddSites.take(50)
    println("sitesBRCA length: " + sitesBRCA.length)

    def parseSample(t: (String, String)): Row = {
      val CpGKV = Map[String, Double]()
      val lines = t._2.split("\n")
      lines.foreach(line => {
        val params = line.split("\t")
        val site = params(4).replace(".", "_")
        //verifica sito
        if (sitesBRCA.contains(site)) {
          CpGKV += (site -> params(5).toDouble)
        }
      })
      val filename = t._1.split("/")
      //      val r = Row(filename(filename.size-1).split("\\.")(0), CpGKV.getOrElse(sitesBRCA(0), null),CpGKV.getOrElse(sitesBRCA(1), null),CpGKV.getOrElse(sitesBRCA(2), null))
      val strs = List[String](filename(filename.size - 1).split("\\.")(0))
      val values = ListBuffer[Double]()
      sitesBRCA.foreach(s => values.append(CpGKV.getOrElse(s, Double.NaN)))
      val r = Row.fromSeq((strs ::: values.toList).toSeq)
      r
    }

    val arrSchemaSample = ArrayBuffer[StructField]()
    arrSchemaSample.append(StructField("manually_curated__opengdc_id", StringType, false))
    sitesBRCA.foreach(s => arrSchemaSample.append(StructField(s, DoubleType, false)))
    val schemaSample = new StructType(arrSchemaSample.toArray)
    println("SAMPLE schema: " + schemaSample)

    var purged = 0
    val rddSample = sc.wholeTextFiles(PATH + "*.bed").map(parseSample).filter(row => {
      val seq = row.toSeq
      var containtNaN = false
      breakable {
        seq.foreach(f => {
          if (f.isInstanceOf[Double] && f.asInstanceOf[Double].isNaN) {
            containtNaN = true
            purged += 1
            break
          }
        })
      }
      !containtNaN
    })
    val dfSample = spark.createDataFrame(rddSample, schemaSample).cache
    println("tissues purged: " + purged)
    println("TISSUE SAMPLE DATAFRAME - total tissues " + dfSample.count())
    dfSample.show

    val dfJoined = dfSample.join(dfMeta, "manually_curated__opengdc_id")
    println("JOINED DATAFRAME")
    dfJoined.show

    //add features based on BRCA relevant genes
    val assembler = new VectorAssembler().setInputCols(sitesBRCA).setOutputCol("features")
    //add new features column
    val dfExtended = assembler.transform(dfJoined)

    //    val labelIndexer2 = new StringIndexer()
    //      .setInputCol("label")
    //      .setOutputCol("indexedLabel")
    //      .fit(dfLabeled)
    //
    //    val featureIndexer2 = new VectorIndexer()
    //      .setInputCol("features")
    //      .setOutputCol("indexedFeatures")
    //      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
    //      .fit(dfLabeled)

    //  split the dataframe into training and test data
    //    val splitSeed = 9912  //fix seed only for repeatable tests
    val splitSeed = new Random().nextInt()
    val Array(trainingData, testData) = dfExtended.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: " + testData.count())

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dfExtended)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(dfExtended)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    //SPARK DOC
    //Classification Models in MLlib: Spark has several models available for performing binary and multiclass classification out of the box. The following models are available for classification in Spark:
    //-Logistic regression
    //-Decision trees (So under the hood, Apache Spark calls the random forest with one tree.)
    //-Random forests
    //-Gradient-boosted trees

    //BIOINF DOC
    //The most popular classication methods that can be applied to gene expression proles are:
    //-C4.5 Classication Tree (C4.5)
    //-Support Vector Machines (SVM)
    //-Random Forest (RF)
    //-Nearest Neighbour
    //-Logic Data Mining (logic classication formulas or rule-based classiers)

    //LOGISTIC REGRESSION CLASSIFIER
    //    // create classifier and set parameters for training
    //    val lr = new LogisticRegression()
    //      .setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //    //  use logistic regression to train (fit) the model with the training data
    //    val modelLR = lr.fit(trainingData)
    //
    //    //coeff and intercept of logistic regression
    //    println(s"Coefficients: ${modelLR.coefficients} Intercept: ${modelLR.intercept}")
    //
    //    // run the  model on test features to get predictions
    //    val predictionsLR = modelLR.transform(testData)
    //    //transformation added new columns: rawPrediction, probability and prediction.
    //    predictionsLR.show()
    //    calculateMetrics("LOGISTIC REGRESSION", predictionsLR)
    //
    //    //A common metric used for logistic regression is area under the ROC curve (AUC). We can use the BinaryClasssificationEvaluator to obtain the AUC
    //    // create an Evaluator fressionor binary classification, which expects two input columns: rawPrediction and label.
    //    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    //    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).
    //    val accuracyLR = evaluator.evaluate(predictionsLR)
    //    println("LOGISTIC REGRESSION accuracy: " + accuracyLR)
    //
    //    val evaluatorLR = new MulticlassClassificationEvaluator()
    //      .setLabelCol("indexedLabel")
    //      .setPredictionCol("prediction")
    //      .setMetricName("accuracy")
    //    val accuracyLR2 = evaluatorLR.evaluate(predictionsLR)
    //    println("LR accuracy: " + accuracyLR2)
    //    println(s"Test Error = ${1.0 - accuracyLR2}")
    //
    //
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Chain indexers and tree in a Pipeline.
    val pipelineLR = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    // Train model. This also runs the indexers.
    val modelLR = pipelineLR.fit(trainingData)

    // Make predictions.
    val predictionsLR = modelLR.transform(testData)
    calculateMetrics("LOGISTIC REGRESSION", predictionsLR)

    // Select example rows to display.
    predictionsLR.select("predictedLabel", "label", "features").show(5)

    //    val evaluator2 = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    //    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).
    //    val accuracyLR2= evaluator2.evaluate(predictionsLR)
    //    println("LOGISTIC REGRESSION accuracy2: " + accuracyLR2)

    //    //DECISION TREES CLASSIFIER
    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Chain indexers and tree in a Pipeline.
    val pipelineDT = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val modelDT = pipelineDT.fit(trainingData)

    // Make predictions.
    val predictionsDT = modelDT.transform(testData)
    calculateMetrics("DECISION TREE", predictionsDT)

    // Select example rows to display.
    predictionsDT.select("predictedLabel", "label", "features").show(5)

    val treeModel = modelDT.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

    //RANDOM FOREST CLASSIFIER
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Chain indexers and forest in a Pipeline.
    val pipelineRF = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val modelRF = pipelineRF.fit(trainingData)

    // Make predictions.
    val predictionsRF = modelRF.transform(testData)

    calculateMetrics("RANDOM FOREST", predictionsRF)

    // Select example rows to display.
    predictionsRF.select("predictedLabel", "label", "features").show(5)

    val rfModel = modelRF.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

    //GRADIENT-BOOSTED TREE CLASSIFIER
    // Train a GBT model.
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

    // Chain indexers and GBT in a Pipeline.
    val pipelineGBT = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val modelGBT = pipelineGBT.fit(trainingData)

    // Make predictions.
    val predictionsGBT = modelGBT.transform(testData)

    calculateMetrics("GRADIENT-BOOSTED TREE", predictionsGBT)

    // Select example rows to display.
    predictionsGBT.select("predictedLabel", "label", "features").show(5)

    val gbtModel = modelGBT.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")

    spark.stop()
  }

  def calculateMetrics(algo: String, dfPrediction: DataFrame) {
    println(s"algo $algo")
    import dfPrediction.sparkSession.implicits._
    val lp = dfPrediction.select("label", "prediction")
    val counttotal = dfPrediction.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    println(s"total:$counttotal, correct:$correct, wrong:$wrong, truP:$trueP, falseP:$falseP, trueN:$trueN, falseN:$falseN, ratioWrong:$ratioWrong, ratioCorrect:$ratioCorrect")

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(dfPrediction)
    println(s"$algo accuracy: " + accuracy)
    println(s"Test Error = ${(1.0 - accuracy)}")

  }
}





//Table 5. Subset of extracted genes, which are specifically related to the cancer under
//study, for each investigated tumor dataset.
//Tumor abbreviation Cancer related
//BRCA
//A2M, ABCB1, ABCC1, ACOT7, ACOX2,
//ADAMTS16, ADAMTS17, ADORA2A, AQP1,
//CA12, CDCP1, CREB3L1, CRYAB, FGF1,
//IL11RA, INHBA, ITIH5, KIF26B, LRRC3B,
//MEG3, MUC1, PRKD1, SDPR