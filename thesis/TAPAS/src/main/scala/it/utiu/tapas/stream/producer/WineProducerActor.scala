package it.utiu.tapas.stream.producer

import akka.actor.Props
import it.utiu.tapas.stream.consumer.AbstractProducerActor
import it.utiu.tapas.util.Consts

object WineProducerActor {
  def props(): Props = Props(new WineProducerActor())
}

class WineProducerActor extends AbstractProducerActor(Consts.CS_WINE, Consts.TOPIC_WINE) {
}