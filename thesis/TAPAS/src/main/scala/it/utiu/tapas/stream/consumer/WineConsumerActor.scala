package it.utiu.tapas.stream.consumer

import akka.actor.ActorRef
import akka.actor.Props
import it.utiu.tapas.util.Consts

object WineConsumerActor {

  def props(predictor: ActorRef): Props = Props(new WineConsumerActor(predictor))
  val header = "Class,Alcohol,Malic,Ash,Alcalinity,Magnesium,phenols,Flavanoids,Nonflavanoid,Proanthocyanins,Color,Hue,OD280,Proline"
  val COLS_NUM = 14
}

class WineConsumerActor(predictor: ActorRef) extends AbstractConsumerActor(Consts.CS_WINE, Consts.TOPIC_WINE, predictor, WineConsumerActor.header, WineConsumerActor.COLS_NUM) {
   override def doInternalConsuming() {}
}