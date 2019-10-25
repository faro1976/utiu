package it.utiu.tapas.ml.predictor

import akka.actor.Props
import it.utiu.tapas.base.AbstractStatsFeederActor
import it.utiu.tapas.util.Consts

object ActivityFeederActor {
  def props(): Props = Props(new ActivityFeederActor())

}

class ActivityFeederActor() extends AbstractStatsFeederActor(Consts.CS_ACTIVITY) {
}