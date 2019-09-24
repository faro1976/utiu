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
import org.apache.spark.mllib.stat.Statistics


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
    import spark.implicits._
    val df3 = df1.select("data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat").toDF()    
//    val df3 = df2.withColumn("hashrate_24h_l", df2.col("hashrate_24h").cast("long"))
//    df3.show    
    
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "volume_24h", "mempool_transactions", "mempool_size", "mempool_tps", "mempool_total_fee_usd", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "average_transaction_fee_usd_24h", "market_price_usd", "next_difficulty_estimate", "suggested_transaction_fee_per_byte_sat")).setOutputCol("features").setHandleInvalid("keep")
    val df4 = assembler.transform(df3)

//    val Row(coeff1: Matrix) = Correlation.corr(df4, "features").head
//println("Pearson correlation matrix:\n" + coeff1.toString)
//
//val Row(coeff2: Matrix) = Correlation.corr(df4, "features", "spearman").head
//println("Spearman correlation matrix:\n" + coeff2.toString)


val Row(coeff1: Matrix) = Correlation.corr(df4, "features").head
println(s"Pearson correlation matrix:\n $coeff1")


val Row(coeff2: Matrix) = Correlation.corr(df4, "features", "spearman").head
println(coeff2.toString(Int.MaxValue, Int.MaxValue))


for (v<-coeff2.colIter) {
  for(i<-v.toArray) {
    println(i)
  }
}
System.exit(0)
    return null
  }

}