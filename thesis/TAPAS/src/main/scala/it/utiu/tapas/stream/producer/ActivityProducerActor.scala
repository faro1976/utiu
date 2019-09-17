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



object ActivityProducerActor {

  def props(): Props = Props(new ActivityProducerActor())

}

class ActivityProducerActor extends AbstractProducerActor(Consts.CS_ACTIVITY, Consts.TOPIC_ACTIVITY) {


}