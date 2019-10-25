package it.utiu.tapas.stream.producer

import akka.actor.Props
import it.utiu.tapas.stream.consumer.AbstractProducerActor
import it.utiu.tapas.util.Consts

object ActivityProducerActor {
  def props(): Props = Props(new ActivityProducerActor())
}

class ActivityProducerActor extends AbstractProducerActor(Consts.CS_ACTIVITY, Consts.TOPIC_ACTIVITY) {
}