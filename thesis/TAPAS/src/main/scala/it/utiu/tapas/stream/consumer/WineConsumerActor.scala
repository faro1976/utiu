package it.utiu.tapas.stream.consumer

import akka.actor.ActorRef
import akka.actor.Props
import it.utiu.tapas.util.Consts

object WineConsumerActor {

  def props(predictor: ActorRef): Props = Props(new WineConsumerActor(predictor))
  val header = "Class,Alcohol,Malic,Ash,Alcalinity,Magnesium,phenols,Flavanoids,Nonflavanoid,Proanthocyanins,Color,Hue,OD280,Proline"

}

class WineConsumerActor(predictor: ActorRef) extends AbstractConsumerActor(Consts.CS_WINE, WineConsumerActor.header, predictor, Consts.TOPIC_WINE) {
   override def doInternalConsuming() {}
}