package it.utiu.tapas.stream.consumer

import akka.actor.Props
import it.utiu.tapas.util.Consts

object WineConsumerActor {

  def props(): Props = Props(new WineConsumerActor())
  val header = "Class,Alcohol,Malic,Ash,Alcalinity,Magnesium,phenols,Flavanoids,Nonflavanoid,Proanthocyanins,Color,Hue,OD280,Proline"
  val COLS_NUM = 14
}

class WineConsumerActor() extends AbstractConsumerActor(Consts.CS_WINE, Consts.TOPIC_WINE, WineConsumerActor.header) {
  override def isPredictionRequest(row: String): Boolean = (row.split(",").size == 13)
}