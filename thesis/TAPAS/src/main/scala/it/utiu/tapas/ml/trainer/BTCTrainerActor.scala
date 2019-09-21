package it.utiu.anavis

import scala.util.Random

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

import akka.actor.Props
import it.utiu.tapas.base.AbstractBaseActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.util.Consts


object BTCTrainerActor {
  def props(): Props =
    Props(new BTCTrainerActor())
}

class BTCTrainerActor extends AbstractTrainerActor(Consts.CS_BTC) {

  override def doInternalTraining(spark: SparkSession): MLWritable = {
    //caricamento dataset come CSV inferendo lo schema dall'header
    val df1 = spark.read.json(HDFS_CS_PATH+"*")
    df1.show
    df1.printSchema()
    
    val df = df1.select("data/average_transaction_fee_24h", "data/average_transaction_fee_usd_24h").toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
println("Pearson correlation matrix:\n" + coeff1.toString)

val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
println("Spearman correlation matrix:\n" + coeff2.toString)

    return null
  }

}