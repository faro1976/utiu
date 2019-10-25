package it.utiu.tapas.ml.predictor

import akka.actor.Props
import it.utiu.tapas.base.AbstractStatsFeederActor
import it.utiu.tapas.util.Consts

object BTCFeederActor {
  def props(): Props = Props(new BTCFeederActor())

}

class BTCFeederActor() extends AbstractStatsFeederActor(Consts.CS_BTC) {
}