package it.utiu.tapas.stream.consumer

import scala.concurrent.ExecutionContext

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

import akka.NotUsed
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import it.utiu.tapas.base.AbstractBaseActor

object AbstractProducerActor {

  case class StartProducing()

  val IN_PATH = "/Users/rob/UniNettuno/dataset/"
}

class AbstractProducerActor(name: String, topic: String) extends AbstractBaseActor {
  override def receive: Receive = {

    case AbstractProducerActor.StartProducing() => doProduce()
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
    //        new ProducerRecord[Array[Byte], String](AbstractProducerActor.topic1, elem)
    //      }
    //      .runWith(Producer.plainSink(producerSettings))

    while (true) {

      val file = scala.io.Source.fromFile(AbstractProducerActor.IN_PATH + name + "/" + name + ".data.input")
      val source: Source[String, NotUsed] = Source(file.getLines().toIterable.to[collection.immutable.Iterable])

      val done = source

        .map(_.toString)
        .map { elem =>
          new ProducerRecord[Array[Byte], String](topic, elem)
        }
        .runWith(Producer.plainSink(producerSettings))

      Thread.sleep(5000);
    }

  }

}