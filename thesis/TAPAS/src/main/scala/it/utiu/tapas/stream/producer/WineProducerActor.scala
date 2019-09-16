package it.utiu.tapas.stream.consumer

import scala.concurrent.ExecutionContext

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import it.utiu.tapas.util.Consts



object WineProducerActor {

  def props(): Props = Props(new WineProducerActor())

}

class WineProducerActor extends AbstractProducerActor(Consts.CS_WINE, Consts.TOPIC_WINE) {


}