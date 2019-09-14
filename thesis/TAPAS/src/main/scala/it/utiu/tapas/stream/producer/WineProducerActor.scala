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



object WineProducerActor {

  def props(): Props = Props(new WineProducerActor())

  case class StartProducing()

  val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val IN_PATH = "/Users/rob/UniNettuno/dataset/wine/wine.data.input"
}

class WineProducerActor extends Actor with ActorLogging {
  override def receive: Receive = {

    case WineProducerActor.StartProducing() => doProduce()
  }

  private def doProduce() {
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
    
//    val done = Source(1 to 100)
//
//      .map(_.toString)
//      .map { elem =>
//        println(2)
//        new ProducerRecord[Array[Byte], String](WineProducerActor.topic1, elem)
//      }
//      .runWith(Producer.plainSink(producerSettings))
      



    val file = scala.io.Source.fromFile(WineProducerActor.IN_PATH)
val source: Source[String, NotUsed] = Source(file.getLines().toIterable.to[collection.immutable.Iterable])

    val done = source

      .map(_.toString)
      .map { elem =>
        println(2)
        new ProducerRecord[Array[Byte], String](WineProducerActor.topic1, elem)
      }
      .runWith(Producer.plainSink(producerSettings))

      
 
  }

}