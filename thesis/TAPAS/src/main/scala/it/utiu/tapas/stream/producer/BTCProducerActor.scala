package it.utiu.tapas.stream.producer

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
import it.utiu.tapas.stream.consumer.AbstractProducerActor



object BTCProducerActor {

  def props(): Props = Props(new BTCProducerActor())

}

class BTCProducerActor extends AbstractProducerActor(Consts.CS_BTC, Consts.TOPIC_BTC) {
}