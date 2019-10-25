package it.utiu.tapas.stream.consumer

import akka.actor.Props
import it.utiu.tapas.util.Consts

object ActivityConsumerActor {

  def props(): Props = Props(new ActivityConsumerActor())
  val COLS_NUM = 9
  val header = ""
}

class ActivityConsumerActor() extends AbstractConsumerActor(Consts.CS_ACTIVITY, Consts.TOPIC_ACTIVITY, ActivityConsumerActor.header) {
  override def isPredictionRequest(row: String): Boolean = row.split(",").size == (ActivityConsumerActor.COLS_NUM - 1)
}