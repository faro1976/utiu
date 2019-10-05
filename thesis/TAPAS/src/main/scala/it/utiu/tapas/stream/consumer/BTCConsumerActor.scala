package it.utiu.tapas.stream.consumer

import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import scala.concurrent.duration._
import org.apache.kafka.common.serialization.StringDeserializer
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import akka.actor.Props
import akka.actor.AbstractActor.Receive
import akka.actor.Actor
import akka.actor.ActorLogging
import it.utiu.anavis.BTCTrainerActor
import it.utiu.tapas.base.AbstractTrainerActor
import akka.actor.ActorRef
import it.utiu.tapas.util.Consts
import com.google.gson.Gson
import scala.util.parsing.json.JSON


object BTCConsumerActor {
  def props(predictor: ActorRef): Props = Props(new BTCConsumerActor(predictor))
  val header=""
}

  class BTCConsumerActor(predictor: ActorRef) extends AbstractConsumerActor(Consts.CS_BTC, Consts.TOPIC_BTC, predictor, BTCConsumerActor.header) {
     override def isPredictionRequest(row: String) : Boolean = true
     override def isAlwaysInput() : Boolean = true
}