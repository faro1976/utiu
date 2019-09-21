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

object ActivityPredictorActor {
  def props(): Props = Props(new ActivityPredictorActor())

}

class ActivityPredictorActor() extends AbstractPredictorActor[LogisticRegressionModel](Consts.CS_ACTIVITY) {

  override def doInternalPrediction(msgs: String, spark: SparkSession, model: Model[LogisticRegressionModel]): String = {
    val lrModel = model.asInstanceOf[LogisticRegressionModel]

    //cast to List[List[Double]]
    val buffInput = new ListBuffer[List[Double]]()
    //    msgs.foreach(m=>buffInput.append(m.split(",").map(_.).toList))
    //    val input :List[List[Double]] =buffInput.toList
    //    val input = msgs.split(",").map(_.).toList
    val tokens = msgs.split(",")
    val input = (tokens(0).toDouble, tokens(1).toDouble, tokens(2).toDouble, tokens(3).toDouble, tokens(4).toDouble, tokens(5).toDouble, tokens(6).toDouble, tokens(7).toDouble)

    import spark.implicits._

    val sentenceData = Seq(input).toDF()
    val assembler = new VectorAssembler().setInputCols(Array("_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8")).setOutputCol("features")
    val ds = assembler.transform(sentenceData)
    ds.show()

    val predictions = lrModel.transform(ds)
    println("prediction from loaded model...")
    predictions.show()
    val ret = predictions.select("predictedLabel").collect().map(_(0)).toList

    return ret.asInstanceOf[List[Double]](0).toString()
  }

  
  override def getAlgo()= LogisticRegressionModel.read
}