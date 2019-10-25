package it.utiu.tapas.stream.consumer

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._
import util.control.Breaks._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import akka.NotUsed
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import it.utiu.tapas.base.AbstractBaseActor
import java.nio.file.FileSystems
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.WatchEvent
import java.nio.file.Path

object AbstractProducerActor {
  //start producing message
  case class StartProducing()
}

class AbstractProducerActor(name: String, topic: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {
    case AbstractProducerActor.StartProducing() => doProduce()
  }

  private def doProduce() {
    log.info("start producing for " + name + "...")
    val watchService = FileSystems.getDefault.newWatchService()
    Paths.get(RT_INPUT_PATH).register(watchService, ENTRY_CREATE)

    while (true) {
      log.info("waiting new files from " + RT_INPUT_PATH + "...")
      val key = watchService.take()
      key.pollEvents().asScala.foreach(e => {
        e.kind() match {
          case ENTRY_CREATE =>
            val dir = key.watchable().asInstanceOf[Path]
            val fullPath = dir.resolve(e.context().toString());
            log.info("what service event received: [" + fullPath + "] created")
            elabFile(fullPath.toString())
          case x =>
            println("?")
        }
      })
      key.reset()
      //wait x seconds before read again
      Thread.sleep(10000);
    }
  }

  private def elabFile(filePath: String) {
    Thread.sleep(500)
    log.info("process file " + filePath)
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
      .withBootstrapServers(AbstractBaseActor.KAFKA_BOOT_SVR)

    val file = scala.io.Source.fromFile(filePath)
    val source: Source[String, NotUsed] = Source(file.getLines().toIterable.to[collection.immutable.Iterable])

    val done = source
      .map(_.toString)
      .map { elem =>
        new ProducerRecord[Array[Byte], String](topic, elem)
      }
      .runWith(Producer.plainSink(producerSettings))
  }
}