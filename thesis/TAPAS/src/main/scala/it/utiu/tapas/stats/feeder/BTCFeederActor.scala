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
import com.google.gson.Gson
import scala.util.parsing.json.JSONObject
import com.google.gson.JsonObject
import java.text.SimpleDateFormat
import it.utiu.tapas.base.AbstractStatsFeederActor

object BTCFeederActor {
  def props(): Props = Props(new BTCFeederActor())

}

class BTCFeederActor() extends AbstractStatsFeederActor(Consts.CS_BTC) {
}