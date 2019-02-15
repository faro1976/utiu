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

object Runner {
  val PATH = "/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/methylation_beta_value/" 
//  val PATH = "/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/sample/"
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

    def parsePatient(t: (String, String)): Row = {
      val patientKV = Map[String, String]()
      val lines = t._2.split("\n")
      lines.map(line => {
        val kv = line.split("\t")
        patientKV += (kv(0) -> kv(1))
      })
      var status: Double = Double.NaN
      patientKV.get("manually_curated__tissue_status").get match {
        case "tumoral" => status = 1.0
        case "normal"  => status = 0.0
        case _         => throw new RuntimeException("tissue status not recognized")
      }
      val r = Row(patientKV.get("manually_curated__opengdc_id").get, patientKV.get("biospecimen__shared__patient_id").get, patientKV.get("manually_curated__tissue_status").get, status)
      r
    }

    val schemaPatient = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      .add(StructField("biospecimen__shared__patient_id", StringType, false))
      .add(StructField("manually_curated__tissue_status", StringType, false))      
      .add(StructField("label", DoubleType, false))

    //uso wholeTextFiles per poter leggere i dati in maniera autocontenuta ad ogni iterazione di map
    val rddPatient = sc.wholeTextFiles(PATH+"*.meta").map(parsePatient)
    val dfPatient = spark.createDataFrame(rddPatient, schemaPatient).cache
    println("PATIENT DATAFRAME - total patients: " + dfPatient.count())
    dfPatient.show()

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
        val site = params(4).replace(".","_")
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
    println("SAMPLE schema: " +schemaSample)
    
    var purged = 0
    val rddSample = sc.wholeTextFiles(PATH+"*.bed").map(parseSample).filter(row=> {
      val seq = row.toSeq
      var containtNaN = false
      breakable { 
        seq.foreach(f=>{
          if (f.isInstanceOf[Double] && f.asInstanceOf[Double].isNaN){
            containtNaN = true
            purged+=1
            break
          }
        })        
      }
      !containtNaN
    })
    val dfSample = spark.createDataFrame(rddSample, schemaSample).cache
    println("tissues purged: "+purged)
    println("TISSUE SAMPLE DATAFRAME - total tissues " + dfSample.count())
    dfSample.show()

    val dfExtended = dfSample.join(dfPatient, "manually_curated__opengdc_id")
    println("JOINED DATAFRAME")
    dfExtended.show()

    //add features based on BRCA relevant genes
    val assembler = new VectorAssembler().setInputCols(sitesBRCA).setOutputCol("features")
    //add new features column
    val dfFeat = assembler.transform(dfExtended)
    dfFeat.show

    //    val labelIndexer = new StringIndexer().setInputCol("status").setOutputCol("label")
    //    val dfLabel = labelIndexer.fit(dfFeat).transform(dfFeat)
    val dfLabel = dfFeat
    dfLabel.show

    //  split the dataframe into training and test data
    val splitSeed = 9912
    val Array(trainingData, testData) = dfLabel.randomSplit(Array(0.7, 0.3), splitSeed)
    println("training set items: " + trainingData.count() + ", test set items: "+ testData.count())

    //SPARK DOC
    //Classification Models in MLlib: Spark has several models available for performing binary and multiclass classification out of the box. The following models are available for classification in Spark:
    //-Logistic regression
    //-Decision trees
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
    // create classifier and set parameters for training
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //  use logistic regression to train (fit) the model with the training data
    val model = lr.fit(trainingData)

    //coeff and intercept of logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // run the  model on test features to get predictions
    val predictions = model.transform(testData)
    //transformation added new columns: rawPrediction, probability and prediction.
    predictions.show()

    //A common metric used for logistic regression is area under the ROC curve (AUC). We can use the BinaryClasssificationEvaluator to obtain the AUC
    // create an Evaluator for binary classification, which expects two input columns: rawPrediction and label.
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    // Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy: " + accuracy)

    //    //DECISION TREES CLASSIFIER
    //    val classifierDT = new DecisionTreeClassifier()
    //    //  use logistic regression to train (fit) the model with the training data**
    //    val modelDT = classifierDT.fit(trainingData)
    //
    //    // run the  model on test features to get predictions
    //    val predictionsDT = modelDT.transform(testData)
    //    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.
    //    predictions.show

    spark.stop()
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