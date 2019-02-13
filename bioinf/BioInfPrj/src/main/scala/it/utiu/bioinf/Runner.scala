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


object Runner {
  def main(args: Array[String]): Unit = {
    println("entro!!")
    val spark = SparkSession.builder
      .appName("Progetto Big Data applicati alla Bioinformatica")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    
     println("post spark context init!!")

    //            id,status,cg13869341,cg14008030,cg06937459,cg16355945,
    //            00a173ee-32df-4747-8b04-05b3913e3118-mbv,tumoral,0.898509278030815,0.75680426983933,0.978631965130895,0.0928966658174382

    val clinics =Array("manually_curated__tissue_status", "biospecimen__shared__patient_id")
    val genesBRCA =Array("A2M", "ABCB1", "ABCC1", "ACOT7", "ACOX2","ADAMTS16", "ADAMTS17", "ADORA2A", "AQP1", "CA12", "CDCP1", "CREB3L1", "CRYAB", "FGF1","IL11RA", "INHBA", "ITIH5", "KIF26B", "LRRC3B", "MEG3", "MUC1", "PRKD1", "SDPR")
    val sitesBRCA = Array("cg12417807","cg11139127","cg27166707")
    def parsePatient(t: (String, String)): Row = {
      println("parse!!")
      val patientKV = Map[String,String]() 
      val lines = t._2.split("\n")
      lines.map(line=>{
        val kv = line.split("\t")
        patientKV += (kv(0) -> kv(1))        
      })
      Row(patientKV.get("manually_curated__opengdc_id").get, patientKV.get("biospecimen__shared__patient_id").get, patientKV.get("manually_curated__tissue_status").get)
    }
    
    def parseSample(t: (String, String)): Row = {
      val CpGKV = Map[String,String]()
      val lines = t._2.split("\n")
      lines.map(line=>{
        val params = line.split("\t")
        //verifica gene
//        if (genesBRCA.contains(params(6))) {
        if (sitesBRCA.contains(params(4))) {        
          CpGKV += (params(4) -> params(5))
        }        
      })
      Row(t._1, sitesBRCA.map(CpGKV.getOrElse(_, "")))
    }
    
        /*.filter(_(6) != "?")*/
        /*.map(_.drop(1))*/
        //        .map(t=>{
        //          t(1) = if (t(1)=="tumoral") "1" else "0"  //definisco la classe di tipo Long
        //          t.drop(0)                                 //elimino campo id campione
        //          t.map(_.toDouble)                         //trasformo campi da string a double
        //        })
//        .map(
//          line => Row(if (line(1) == "tumoral") 1.0 else 0.0, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble))
//    }
//
//    def parseSample(rdd: RDD[String]): RDD[Row] = {
//      rdd.map(_.split(";"))
//        /*.filter(_(6) != "?")*/
//        /*.map(_.drop(1))*/
//        //        .map(t=>{
//        //          t(1) = if (t(1)=="tumoral") "1" else "0"  //definisco la classe di tipo Long
//        //          t.drop(0)                                 //elimino campo id campione
//        //          t.map(_.toDouble)                         //trasformo campi da string a double
//        //        })
//        .map(
//          line => Row(if (line(1) == "tumoral") 1.0 else 0.0, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble))
//    }
    
    
        val schemaPatient = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      .add(StructField("biospecimen__shared__patient_id", StringType, false))
      .add(StructField("manually_curated__tissue_status", StringType, false))

        val schemaSample = new StructType()
      .add(StructField("manually_curated__opengdc_id", StringType, false))
      
      sitesBRCA.map(s=>schemaSample.add(StructField(s, DoubleType, false)))
      
      
      
    //uso wholeTextFiles per poter leggere i dati in maniera autocontenuta ad ogni iterazione di map
      println("whole!!")
    val rddPatient = sc.wholeTextFiles("/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/*.meta").map(parsePatient)
    val dfPatient = spark.createDataFrame(rddPatient, schemaPatient)
    dfPatient.show()
    
    val rddSample = sc.wholeTextFiles("/Users/robertofavaroni/UniNettuno/courses/Big Data applicati alla bioinformatica/project/dataset/tcga-brca/*.bed").map(parseSample)
    val dfSample = spark.createDataFrame(rddSample, schemaSample)
    dfSample.show()
    
    
//    val dfPatient = parsePatient(rddPatient)
//    //    val obsRDD = parseRDD(rdd).map(parseObs)
//    val obsRDD = parseRDD(rdd)
//
//    //val rowsRdd: RDD[Row] = sc.parallelize(
//    //  Seq(
//    //    Row("first", 2.0, 7.0),
//    //    Row("second", 3.5, 2.5),
//    //    Row("third", 7.0, 5.9)
//    //  )
//    //)
//    val schema = new StructType()
//      .add(StructField("class", DoubleType, false))
//      .add(StructField("cg13869341", DoubleType, true))
//      .add(StructField("cg14008030", DoubleType, true))
//      .add(StructField("cg06937459", DoubleType, true))
//      .add(StructField("cg16355945", DoubleType, true))
//      
//      
//
//    //val fields = schemaString.split(" ")
//    //  .map(fieldName => StructField(fieldName, StringType, nullable = true))
//    //val schema = StructType(fields)
//    val df = spark.createDataFrame(obsRDD, schema)
//
//    df.show()
//    df.describe().show()
//    df.printSchema()
//
//    //    val obsDF = spark.createDataFrame(rowsRdd)
//    //    val obsDF = obsRDD.toDF().cache()
//    //obsDF.registerTempTable("obs")
//
//    //define features
//    val featureCols = Array("cg06937459", "cg14008030")
//
//    //set the input and output column names**
//    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
//    //return a dataframe with all of the  feature columns in  a vector column**
//    val df2 = assembler.transform(df)
//    // the transform method produced a new column: features.
//    df2.show
//
//    //  Create a label column with the StringIndexer
//    val labelIndexer = new StringIndexer().setInputCol("class").setOutputCol("label")
//    val df3 = labelIndexer.fit(df2).transform(df2)
//    // the  transform method produced a new column: label.
//    df3.show
//
//    //  split the dataframe into training and test data
//    val splitSeed = 5043
//    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
//
//    // create the classifier,  set parameters for training
//    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
//    //  use logistic regression to train (fit) the model with the training data**
//    val model = lr.fit(trainingData)
//
//    // Print the coefficients and intercept for logistic regression
//    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
//
//    // run the  model on test features to get predictions
//    val predictions = model.transform(testData)
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