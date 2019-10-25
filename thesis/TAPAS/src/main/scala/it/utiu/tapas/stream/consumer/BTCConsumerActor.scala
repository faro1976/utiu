package it.utiu.tapas.stream.consumer

import akka.actor.Props
import it.utiu.tapas.util.Consts

object BTCConsumerActor {
  def props(): Props = Props(new BTCConsumerActor())
  val header = ""
}

class BTCConsumerActor() extends AbstractConsumerActor(Consts.CS_BTC, Consts.TOPIC_BTC, BTCConsumerActor.header) {
  override def isPredictionRequest(line: String) = {
    true
  }

  override def isAlwaysInput(): Boolean = true
}