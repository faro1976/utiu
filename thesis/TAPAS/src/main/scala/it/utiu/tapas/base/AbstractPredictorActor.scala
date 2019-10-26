package it.utiu.tapas.base

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLReader
import org.apache.spark.sql.SparkSession
import it.utiu.tapas.base.AbstractPredictorActor.AskPrediction
import it.utiu.tapas.base.AbstractPredictorActor.TellPrediction
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.PipelineModel
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.RandomForestClassificationModel

object AbstractPredictorActor {
  //ask prediction message
  case class AskPrediction(msgs: String)
  //tell prediction message
  case class TellPrediction(prediction: String, input: String)
}

abstract class AbstractPredictorActor(name: String) extends AbstractBaseActor(name) {
  var mlModel: Transformer = null

  initSpark("predictor", SPARK_URL_PREDICTION)

  override def receive: Receive = {
    case AskPrediction(msgs: String) =>
      val prediction = doPrediction(msgs)
      if (prediction != null) sender ! TellPrediction(prediction, getInput(msgs))

    case AbstractTrainerActor.TrainingFinished(model: Transformer) =>
      mlModel = model.asInstanceOf[Transformer];
      log.info("reloaded model " + mlModel + " just built")
  }

  def doInternalPrediction(msgs: String, spark: SparkSession, model: Transformer): String

  def getInput(msg: String): String = msg

  private def doPrediction(msgs: String): String = {
    log.info("start prediction...")

    if (mlModel == null) {
      if (!Files.exists(Paths.get(ML_MODEL_FILE))) return null
      mlModel = loadModelFromDisk()
    }

    //invoke internal
    val prediction = doInternalPrediction(msgs, spark, mlModel)

    return prediction
  }

  private def loadModelFromDisk(): Transformer = {
    log.info("restoring model " + ML_MODEL_FILE_COPY + " from disk...")
    //delete old copy-of-model
    FileUtils.deleteDirectory(new File(ML_MODEL_FILE_COPY))
    //create a fresh copy-of-model
    FileUtils.copyDirectory(new File(ML_MODEL_FILE), new File(ML_MODEL_FILE_COPY), true);
    //load copy-of-model
    val algo = scala.io.Source.fromFile(ML_MODEL_FILE + ".algo").getLines().next()
    algo match {
      case "org.apache.spark.ml.regression.LinearRegressionModel"               => LinearRegressionModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.regression.DecisionTreeRegressorModel"          => DecisionTreeRegressionModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.regression.RandomForestRegressionModel"         => RandomForestRegressionModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.regression.GBTRegressionModel"                  => GBTRegressionModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.classification.LogisticRegressionModel"         => LogisticRegressionModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.classification.DecisionTreeClassificationModel" => DecisionTreeClassificationModel.read.load(ML_MODEL_FILE_COPY)
      case "org.apache.spark.ml.classification.RandomForestClassificationModel" => RandomForestClassificationModel.read.load(ML_MODEL_FILE_COPY)
    }
    //      PipelineModel.load(ML_MODEL_FILE_COPY)
  }
}