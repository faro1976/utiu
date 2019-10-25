package it.utiu.tapas.stream.producer

import akka.actor.Props
import it.utiu.tapas.stream.consumer.AbstractProducerActor
import it.utiu.tapas.util.Consts

object BTCProducerActor {
  def props(): Props = Props(new BTCProducerActor())
}

class BTCProducerActor extends AbstractProducerActor(Consts.CS_BTC, Consts.TOPIC_BTC) {
}