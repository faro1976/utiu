package it.utiu.tapas.base
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.ml.evaluation.RegressionEvaluator
import it.utiu.tapas.util.Consts
import java.util.Date
import java.nio.file.StandardOpenOption

abstract class AbstractRegressionTrainerActor(name: String) extends AbstractTrainerActor(name) {
  
override def calculateMetrics(algo: String, predictions: DataFrame, rows: (Long, Long)): Double = {
    
    //print ml evaluation
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    log.info(s"$algo - Root mean squared error: $rmse")

    //print ml metrics
    evaluator.setMetricName("mse")
    val mse = evaluator.evaluate(predictions)
    log.info(s"$algo - Mean squared error: $mse")

    evaluator.setMetricName("r2")
    val r2 = evaluator.evaluate(predictions)
    log.info(s"$algo - r2: $r2")

    evaluator.setMetricName("mae")
    val mae = evaluator.evaluate(predictions)
    log.info(s"$algo - Mean absolute error: $mae")   
    
    val str = tmstFormat.format(new Date()) + "," + algo + "," + r2 + "," + rows._1 + "," + rows._2 + "\n"
    writeFile(RT_OUTPUT_PATH + Consts.CS_BTC + "-regression-eval.csv", str, Some(StandardOpenOption.APPEND))
    
    r2
  }  
}