package it.utiu.bioinf

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import org.apache.spark.sql.functions
import scala.collection.mutable.Map
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.log4j.lf5.LogLevel
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer


object Runner {
  def main(args: Array[String]): Unit = {
    println("entro!!")
    val spark = SparkSession.builder
      .appName("Progetto Big Data applicati alla Bioinformatica")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
     println("post spark context init!!")

    //            id,status,cg13869341,cg14008030,cg06937459,cg16355945,
    //            00a173ee-32df-4747-8b04-05b3913e3118-mbv,tumoral,0.898509278030815,0.75680426983933,0.978631965130895,0.0928966658174382

    val clinics =Array("manually_curated__tissue_status", "biospecimen__shared__patient_id")
    val genesBRCA =Array("A2M", "ABCB1", "ABCC1", "ACOT7", "ACOX2","ADAMTS16", "ADAMTS17", "ADORA2A", "AQP1", "CA12", "CDCP1", "CREB3L1", "CRYAB", "FGF1","IL11RA", "INHBA", "ITIH5", "KIF26B", "LRRC3B", "MEG3", "MUC1", "PRKD1", "SDPR")
//    val sitesBRCA = Array("cg12417807","cg11139127","cg27166707")
    
    
    
    def parsePatient(t: (String, String)): Row = {
      println("parse!!")
      val patientKV = Map[String,String]() 
      val lines = t._2.split("\n")
      lines.map(line=>{
        val kv = line.split("\t")
        patientKV += (kv(0) -> kv(1))        
      })
      var status:Double = Double.NaN
      patientKV.get("manually_curated__tissue_status").get match {
        case "tumoral" => status = 1.0 
         case "normal" => status = 0.0
         case  _ => throw new RuntimeException("tissue status not recognized")
      }
      val r = Row(patientKV.get("manually_curated__opengdc_id").get, patientKV.get("biospecimen__shared__patient_id").get, status)
      println(r)
      r
    }
    

    

        val schemaPatient = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      .add(StructField("biospecimen__shared__patient_id", StringType, false))
      .add(StructField("label", DoubleType, false))

      
      
    //uso wholeTextFiles per poter leggere i dati in maniera autocontenuta ad ogni iterazione di map
      println("whole!!")
    val rddPatient = sc.wholeTextFiles("/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/*.meta").map(parsePatient)
    val dfPatient = spark.createDataFrame(rddPatient, schemaPatient).cache
    dfPatient.show()
    
    //TODO ROB: un modo più elegante e semplice per sapere associazione siti->gene??
    val rddSites = sc.textFile("/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/*.bed").map(r=>{
      val vals = r.split("\t")
      var ret = ""
      if (genesBRCA.contains(vals(6))) {
        ret = vals(4)
      }
      ret
    }).filter(_!="").distinct()
    //TODO ROB: fare dimensionality (features) reduction!
    val sitesBRCA = rddSites.take(10)
    println("sitesBRCA length: " + sitesBRCA.length)
    
    def parseSample(t: (String, String)): Row = {
      val CpGKV = Map[String,Double]()
      val lines = t._2.split("\n")
      lines.foreach(line=>{
        val params = line.split("\t")
        //verifica sito
        if (sitesBRCA.contains(params(4))) {        
          CpGKV += (params(4) -> params(5).toDouble)
        }        
      }) 
      val filename = t._1.split("/")
//      val r = Row(filename(filename.size-1).split("\\.")(0), CpGKV.getOrElse(sitesBRCA(0), null),CpGKV.getOrElse(sitesBRCA(1), null),CpGKV.getOrElse(sitesBRCA(2), null))
      val strs = List[String](filename(filename.size-1).split("\\.")(0)) 
      val values = ListBuffer[Double]()
      sitesBRCA.foreach(s=>values.append(CpGKV.getOrElse(s, Double.NaN)))
      val r = Row.fromSeq((strs ::: values.toList).toSeq)
      println("row sample: "+r)
      r
    }
        
//        val schemaSample = new StructType()
        val arrSchemaSample = ArrayBuffer[StructField]()
//      .add(StructField("manually_curated__opengdc_id", StringType, false))
//      .add(StructField(sitesBRCA(0), DoubleType, true))
//      .add(StructField(sitesBRCA(1), DoubleType, true))
//      .add(StructField(sitesBRCA(2), DoubleType, true))      
//      sitesBRCA.foreach(s=>schemaSample.add(StructField(s, DoubleType, false)))
        arrSchemaSample.append(StructField("manually_curated__opengdc_id", StringType, false))
        sitesBRCA.foreach(s=>arrSchemaSample.append(StructField(s, DoubleType, false)))
        val schemaSample = new StructType(arrSchemaSample.toArray)
      println(schemaSample)
     
      
      
      
        
    val rddSample = sc.wholeTextFiles("/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/*.bed").map(parseSample)
    val dfSample = spark.createDataFrame(rddSample, schemaSample).cache
    dfSample.show()

    val dfExtended = dfSample.join(dfPatient, "manually_curated__opengdc_id")
    dfExtended.show()
    
    //set the input and output column names**
    val assembler = new VectorAssembler().setInputCols(sitesBRCA).setOutputCol("features")
    //return a dataframe with all of the  feature columns in  a vector column**
    val dfFeat = assembler.transform(dfExtended)
    // the transform method produced a new column: features.
    dfFeat.show

    //  Create a label column with the StringIndexer
//    val labelIndexer = new StringIndexer().setInputCol("status").setOutputCol("label")
//    val dfLabel = labelIndexer.fit(dfFeat).transform(dfFeat)
    val dfLabel = dfFeat
    // the  transform method produced a new column: label.
    dfLabel.show

    //  split the dataframe into training and test data
    val splitSeed = 5043
    val Array(trainingData, testData) = dfLabel.randomSplit(Array(0.7, 0.3), splitSeed)

    
   //QUI FARE PROVE PER DIFFERENTI CLASSIFICATORI!! 
//Classification Models in MLlib: Spark has several models available for performing binary and multiclass classification out of the box. The following models are available for classification in Spark:
//-Logistic regression
//-Decision trees
//-Random forests
//-Gradient-boosted trees
    
    
    //LOGISTIC REGRESSION CLASSIFIER
    // create the classifier,  set parameters for training
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //  use logistic regression to train (fit) the model with the training data**
    val model = lr.fit(trainingData)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // run the  model on test features to get predictions
    val predictions = model.transform(testData)
    //As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.
    predictions.show(100)
    
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