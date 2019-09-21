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


}

class AbstractProducerActor(name: String, topic: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {

    case AbstractProducerActor.StartProducing() => doProduce()
  }

  private def doProduce() {
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
      .withBootstrapServers(AbstractBaseActor.KAFKA_BOOT_SVR)

    while (true) {

      val file = scala.io.Source.fromFile(RT_INPUT_FILE)
      val source: Source[String, NotUsed] = Source(file.getLines().toIterable.to[collection.immutable.Iterable])

      val done = source

        .map(_.toString)
        .map { elem =>
          new ProducerRecord[Array[Byte], String](topic, elem)
        }
        .runWith(Producer.plainSink(producerSettings))

      Thread.sleep(10000);
    }

  }

}