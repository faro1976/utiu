package it.utiu.tapas.base

import it.utiu.tapas.base.AbstractAnalyzerActor.AskTimeSeries
import it.utiu.tapas.base.AbstractAnalyzerActor.TellTimeSeries

object AbstractAnalyzerActor {
  case class AskTimeSeries()
  case class TellTimeSeries(
    name: String, labels: List[String],
    values: List[(String, List[Double])])
}

abstract class AbstractAnalyzerActor(name: String) extends AbstractBaseActor(name) {

  override def receive: Receive = {

    case AskTimeSeries() =>
      val ret = doAnalysis()
      sender ! TellTimeSeries(ret._1, ret._2, ret._3)
  }

  def doInternalAnalysis(): (String, List[String], List[(String, List[Double])])

  private def doAnalysis(): (String, List[String], List[(String, List[Double])]) = {

    return doInternalAnalysis()
  }
}