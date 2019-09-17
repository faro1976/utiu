package it.utiu.tapas.stream.consumer

import akka.actor.ActorRef
import akka.actor.Props
import it.utiu.tapas.util.Consts

object ActivityConsumerActor {

  def props(predictor: ActorRef): Props = Props(new ActivityConsumerActor(predictor))
  val header = ""
  val COLS_NUM = 9
}

class ActivityConsumerActor(predictor: ActorRef) extends AbstractConsumerActor(Consts.CS_ACTIVITY, Consts.TOPIC_ACTIVITY, predictor, ActivityConsumerActor.header, ActivityConsumerActor.COLS_NUM) {
   override def doInternalConsuming() {}
}