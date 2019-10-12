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
import java.text.SimpleDateFormat
import java.nio.file.StandardOpenOption
import AbstractConsumerActor._
import java.util.Date


object BTCConsumerActor {
  def props(predictor: ActorRef, analyzer: ActorRef): Props = Props(new BTCConsumerActor(predictor, analyzer))
  val header=""
}

class BTCConsumerActor(predictor: ActorRef, analyzer: ActorRef) extends AbstractConsumerActor(Consts.CS_BTC, Consts.TOPIC_BTC, predictor, analyzer, BTCConsumerActor.header) {
     override def isPredictionRequest(line: String): Boolean = {
       val map: Map[String, Any] = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]
       val since = map.get("context").asInstanceOf[Map[String, Any]].get("since").get.asInstanceOf[String]
       val usd = map.get("data").asInstanceOf[Map[String, Any]].get("market_price_usd").get.asInstanceOf[String]
       val tSince = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(since)
       val row = tmstFormat.format(new Date()) + "," + tmstFormat.format(tSince) + "," + usd + "\n" 
       writeFile(RT_OUTPUT_FILE+".hit.csv", row.toString, StandardOpenOption.TRUNCATE_EXISTING)
       true
     }
     override def isAlwaysInput() : Boolean = true
}