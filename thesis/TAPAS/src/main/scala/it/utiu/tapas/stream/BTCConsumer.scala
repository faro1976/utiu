package it.utiu.tapas.stream

import akka.kafka.ConsumerSettings
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import akka.kafka.ProducerSettings
import akka.kafka.Subscriptions
import akka.kafka.CommitterSettings
import akka.NotUsed
import akka.kafka.scaladsl.Consumer
import akka.Done
import scala.concurrent.duration._
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import akka.actor.ActorSystem
import scala.concurrent.Future
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord
import akka.kafka.ProducerMessage
import akka.stream.scaladsl.Keep
import akka.kafka.scaladsl.Producer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.ActorMaterializer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext

object BTCConsumer {
  val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("rob")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = system.dispatcher

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val done =
      Consumer.committableSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsync(1) { msg =>
          println(msg)
          println(msg.record.key+":"+msg.record.value)
          msg.committableOffset.commitScaladsl()
        }
        .runWith(Sink.ignore)

    done.onComplete(_ => system.terminate())
  }

}