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
import org.apache.spark.ml.Transformer

object WinePredictorActor {
  def props(): Props = Props(new WinePredictorActor())

}

class WinePredictorActor() extends AbstractPredictorActor(Consts.CS_WINE) {

  override def doInternalPrediction(msgs: String, spark: SparkSession, model: Transformer): String = {
    //cast to List[List[Double]]
    val buffInput = new ListBuffer[List[Double]]()
    val tokens = msgs.split(",")
    val input = (tokens(0).toDouble, tokens(1).toDouble, tokens(2).toDouble, tokens(3).toDouble, tokens(4).toDouble, tokens(5).toDouble, tokens(6).toDouble, tokens(7).toDouble, tokens(8).toDouble, tokens(9).toDouble, tokens(10).toDouble, tokens(11).toDouble, tokens(12).toDouble)
    import spark.implicits._
    val sentenceData = Seq(input).toDF("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")
    val assembler = new VectorAssembler().setInputCols(Array("Alcohol", "Malic", "Ash", "Alcalinity", "Magnesium", "phenols", "Flavanoids", "Nonflavanoid", "Proanthocyanins", "Color", "Hue", "OD280", "Proline")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)
    ds.show()

    val predictions = model.transform(ds)
    println("prediction from loaded model...")
    predictions.show()
    val ret = predictions.select("prediction").collect().map(_(0)).toList

    return ret.asInstanceOf[List[Double]](0).toString()
  }
}