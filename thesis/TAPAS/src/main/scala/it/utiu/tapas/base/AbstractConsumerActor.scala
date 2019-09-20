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
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import it.utiu.tapas.base.AbstractBaseActor
import it.utiu.tapas.base.AbstractPredictorActor

import it.utiu.tapas.stream.consumer.AbstractConsumerActor._

object AbstractConsumerActor {

  case class StartConsuming()

  
  val BUFF_SIZE = 5
 
  
}

abstract class AbstractConsumerActor(name: String, topic: String, predictor: ActorRef, header: String, colsNum: Int) extends AbstractBaseActor(name) {
  override def receive: Receive = {

    case AbstractConsumerActor.StartConsuming()             => doConsuming()
    case AbstractPredictorActor.TellPrediction(prediction) => println("received prediction: " + prediction)
  }

  val buffer = ArrayBuffer[String]()
  def doInternalConsuming()
  
  private def doConsuming() {


    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher

    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers(AbstractBaseActor.kafkaBootstrapServers)
      .withGroupId(AbstractBaseActor.groupId)
    //      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")  //read from beginnig

    val done =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
        .mapAsync(1) { msg =>
          val strMsg = msg.value
          println(s"value: ${strMsg}")
          val tokens = strMsg.split(",")
          if (tokens.size == colsNum) {
            //messaggio consuntivo
            buffer.append(strMsg)
            if (buffer.size == BUFF_SIZE) {
              try {
                val path = new Path(HDFS_CS_PATH+name+"."+new Date().getTime)
                val conf = new Configuration()
                val fs = FileSystem.get(new URI(AbstractBaseActor.HDFS_URL), conf);
                val out = fs.create(path)
                val pw = new PrintWriter(out)
                if (header!=null&header.size>0) pw.write(header + Properties.lineSeparator)
                buffer.foreach(i => pw.write(i + Properties.lineSeparator))
                pw.close()
                out.close()

              } catch {
                case t: Throwable => println(t)
              }
              buffer.clear()
            }
          } else {
            //messaggio preventivo
            println("request prediction for: " + strMsg)
            predictor ! AbstractPredictorActor.AskPrediction(strMsg)
          }
          Future.successful(Done)
        }
        .runWith(Sink.ignore)

    done.onComplete(_ => return )

  }

  private def reloadModel() {
  }

}