package it.utiu.tapas.stream.consumer

import akka.actor.ActorRef
import akka.actor.Props
import it.utiu.tapas.util.Consts

object ActivityConsumerActor {

  def props(predictor: ActorRef, analyzer: ActorRef): Props = Props(new ActivityConsumerActor(predictor, analyzer))
  val COLS_NUM = 9
  val header = ""
}

class ActivityConsumerActor(predictor: ActorRef, analyzer: ActorRef) extends AbstractConsumerActor(Consts.CS_ACTIVITY, Consts.TOPIC_ACTIVITY, predictor, analyzer, ActivityConsumerActor.header) {
   override def isPredictionRequest(row: String) : Boolean = row.split(",").size == (ActivityConsumerActor.COLS_NUM-1)
}