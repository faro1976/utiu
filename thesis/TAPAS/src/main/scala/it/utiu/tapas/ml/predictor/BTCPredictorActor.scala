package it.utiu.tapas.ml.predictor

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.reflect.api.materializeTypeTag

import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import akka.actor.Props
import it.utiu.tapas.base.AbstractPredictorActor
import it.utiu.tapas.util.Consts
import org.apache.spark.ml.util.MLReader
import org.apache.spark.ml.regression.GBTRegressionModel
import scala.util.parsing.json.JSON

object BTCPredictorActor {
  def props(): Props = Props(new BTCPredictorActor())

}

class BTCPredictorActor() extends AbstractPredictorActor[GBTRegressionModel](Consts.CS_BTC) {

  override def doInternalPrediction(msgs: String, spark: SparkSession, model: Model[GBTRegressionModel]): String = {
    val modelGBT = model.asInstanceOf[GBTRegressionModel]

    import spark.implicits._ // spark is your SparkSession object
    val df1 = spark.read.json(Seq(msgs).toDS)
    val df2 = df1.select("context.cache.since", "data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat", /*"prev_avg_price", */"market_price_usd")).setOutputCol("features")
      .setHandleInvalid("skip")
    val df3 = assembler.transform(df2)      
    
    val predictions = modelGBT.transform(df3)
    predictions.show()
    val ret = predictions.select("prediction").collect().map(_(0)).toList
    return ret.asInstanceOf[List[Double]](0).toString()
  }

  
  override def getAlgo()= GBTRegressionModel.read
  
  override def getInput(msg: String): String = {
    val map: Map[String, Any] = JSON.parseFull(msg).get.asInstanceOf[Map[String, Any]]
    map.get("context").asInstanceOf[Map[String, Any]].get("since").get.asInstanceOf[String]
  }
}