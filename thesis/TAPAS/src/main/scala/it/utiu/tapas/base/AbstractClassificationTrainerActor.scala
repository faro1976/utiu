package it.utiu.tapas.base
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import java.nio.file.StandardOpenOption
import it.utiu.tapas.util.Consts
import java.util.Date

abstract class AbstractClassificationTrainerActor(name: String) extends AbstractTrainerActor(name) {


  override def calculateMetrics(algo: String, predictions: DataFrame, rows: (Long, Long)): Double = {
    import predictions.sparkSession.implicits._
    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    
    val str = tmstFormat.format(new Date()) + "," + algo + "," + (accuracy +","+counttotal+","+correct+","+wrong+","+trueP+","+falseP+","+trueN+","+falseN+","+ratioWrong+","+ratioCorrect) + "," + rows._1 + "," + rows._2 + "\n"
    writeFile(RT_OUTPUT_PATH + Consts.CS_BTC + "-classification-eval.csv", str, Some(StandardOpenOption.APPEND))
    accuracy
  }
}