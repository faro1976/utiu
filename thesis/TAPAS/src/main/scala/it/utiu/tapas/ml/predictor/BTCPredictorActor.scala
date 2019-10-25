package it.utiu.tapas.ml.predictor

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.reflect.api.materializeTypeTag

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

import akka.actor.Props
import it.utiu.tapas.base.AbstractPredictorActor
import it.utiu.tapas.util.Consts
import org.apache.spark.ml.util.MLReader
import com.google.gson.Gson
import scala.util.parsing.json.JSONObject
import com.google.gson.JsonObject
import java.text.SimpleDateFormat
import org.apache.spark.ml.Transformer
import java.time.ZoneId
import java.util.Date

object BTCPredictorActor {
  def props(): Props = Props(new BTCPredictorActor())

}

class BTCPredictorActor() extends AbstractPredictorActor(Consts.CS_BTC) {

  override def doInternalPrediction(msgs: String, spark: SparkSession, model: Transformer): String = {
    import spark.implicits._
    val df1 = spark.read.json(Seq(msgs).toDS)
    val df2 = df1.select("context.cache.since", "data.transactions_24h", "data.difficulty", "data.volume_24h", "data.mempool_transactions", "data.mempool_size", "data.mempool_tps", "data.mempool_total_fee_usd", "data.average_transaction_fee_24h", "data.nodes", "data.inflation_usd_24h", "data.average_transaction_fee_usd_24h", "data.market_price_usd", "data.next_difficulty_estimate", "data.suggested_transaction_fee_per_byte_sat")
    val assembler = new VectorAssembler().setInputCols(Array("transactions_24h", "difficulty", "mempool_transactions", "average_transaction_fee_24h", "nodes", "inflation_usd_24h", "suggested_transaction_fee_per_byte_sat", /*"prev_avg_price", */ "market_price_usd")).setOutputCol("features")
      .setHandleInvalid("skip")
    val df3 = assembler.transform(df2)

    val predictions = model.transform(df3)
    predictions.show()
    val ret = predictions.select("prediction").collect().map(_(0)).toList
    return ret.asInstanceOf[List[Double]](0).toString()
  }

  override def getInput(line: String): String = {
    val gson = new Gson()
    val jobj = gson.fromJson(line, classOf[JsonObject])
    val since = jobj.getAsJsonObject("context").getAsJsonObject("cache").get("since").getAsString
    val tSince = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(since)
    //shift to the next hour interval
    val tSinceNextHour = Date.from(tSince.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().plusHours(1).atZone(ZoneId.systemDefault()).toInstant());
    new SimpleDateFormat("yyyy-MM-dd HH").format(tSinceNextHour)
  }
}