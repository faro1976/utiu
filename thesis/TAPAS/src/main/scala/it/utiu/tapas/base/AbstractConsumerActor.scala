package it.utiu.tapas.stream.consumer

import java.io.PrintWriter
import java.net.URI
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

import akka.Done
import akka.actor.ActorRef
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import it.utiu.tapas.base.AbstractBaseActor
import it.utiu.tapas.base.AbstractPredictorActor
import it.utiu.tapas.stream.consumer.AbstractConsumerActor.BUFF_SIZE
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import org.apache.hadoop.fs.Path


object AbstractConsumerActor {
  //start consuming message
  case class StartConsuming()
  //max buffered items to store 
  val BUFF_SIZE = 5
}


abstract class AbstractConsumerActor(name: String, topic: String, predictor: ActorRef, header: String) extends AbstractBaseActor(name) {
  override def receive: Receive = {
    //start consuming message
    case AbstractConsumerActor.StartConsuming()            => doConsuming()
    //received prediction message
    case AbstractPredictorActor.TellPrediction(prediction, input) => 
      log.info("received prediction: " + prediction)
      val txtOut = new Date() + "@" + prediction + "@" + input + "\n"
      val path = Paths.get(RT_OUTPUT_FILE)
      if (!Files.exists(path)) Files.createFile(path) 
      Files.write(path, txtOut.getBytes, StandardOpenOption.APPEND)
  }

  //buffered messages to store
  val buffer = ArrayBuffer[String]()
  
  //internal
  def isPredictionRequest(row: String) : Boolean = false
  def isAlwaysInput() : Boolean = false
  
  private def doConsuming() {
    log.info("start consuming for "+name+"...")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher

    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(AbstractBaseActor.KAFKA_BOOT_SVR)
      .withGroupId(AbstractBaseActor.KAFKA_GROUP_ID)
    //      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")  //read from beginnig

    val done =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1) { msg =>
          val strMsg = msg.value
          log.info(s"receviced message value: ${strMsg}")
          val isPredictionReq = isPredictionRequest(strMsg)
          if (!isPredictionReq || isAlwaysInput) {
            //input for training action
            buffer.append(strMsg)
            if (buffer.size == BUFF_SIZE) {
              //dump data to HDFS
              log.info("dump " + buffer.size + " input messages to HDFS")
              try {
                val path = new Path(HDFS_CS_INPUT_PATH + name + ".input." + new Date().getTime)
                val conf = new Configuration()
                val fs = FileSystem.get(new URI(AbstractBaseActor.HDFS_URL), conf);
                val out = fs.create(path)
                val pw = new PrintWriter(out)
                if (header != null & header.size > 0) pw.write(header + Properties.lineSeparator)
                buffer.foreach(i => pw.write(i + Properties.lineSeparator))
                pw.close()
                out.close()

              } catch {
                case t: Throwable => log.info(t.toString)
              }
              buffer.clear()
            } else log.info("input messages buffered")
          }
          if (isPredictionReq) {
            //input for prediction action
            log.info("request prediction for: " + strMsg)
            predictor ! AbstractPredictorActor.AskPrediction(strMsg)
          }
          Future.successful(Done)
        }
        .runWith(Sink.ignore)

    done.onComplete(_ => return )
  }
}