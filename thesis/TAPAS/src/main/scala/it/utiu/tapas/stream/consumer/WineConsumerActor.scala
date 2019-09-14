package it.utiu.tapas.stream.consumer



import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import it.utiu.tapas.ml.predictor.WineForecasterActor
import akka.kafka.Subscriptions
import java.io.PrintWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import akka.kafka.scaladsl.Consumer
import org.apache.hadoop.fs.FileSystem
import akka.Done
import akka.stream.scaladsl.Sink
import scala.concurrent.Future
import java.util.Date
import scala.util.Properties


object WineConsumerActor {

  def props(forecaster: ActorRef): Props = Props(new WineConsumerActor(forecaster))

  case class StartConsuming()

  val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"

  val COLS_NUM = 14
  val BUFF_SIZE = 5

  val header = "Class,Alcohol,Malic,Ash,Alcalinity,Magnesium,phenols,Flavanoids,Nonflavanoid,Proanthocyanins,Color,Hue,OD280,Proline"
  val buffer = ArrayBuffer[String]()
  val HDFS_PATH = "hdfs://localhost:9000/wine/wine.data"

}

class WineConsumerActor(forecaster: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {

    case WineConsumerActor.StartConsuming() => doConsume()
    case WineForecasterActor.TellPrediction(prediction) => println("received prediction: "+prediction)
  }

  private def doConsume() {

//    val d1 = List(13.39, 1.77, 2.62, 16.1, 90, 2.85, 2.94, .34, 1.45, 4.8, .92, 3.22, 1009)
//    val d2 = List(12.79, 2.67, 2.45, 22, 112, 1.48, 1.36, .24, 1.26, 10.8, .48, 1.47, 344)
//    val d3 = List(12.15, 2.67, 2.43, 22, 112, 1.48, 1.36, .24, 1.26, 10.8, .48, 1.47, 344)
//
//    val msgs = List(d1, d2, d3)
    

    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    import WineConsumerActor._

    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(groupId)
//      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")  //read from beginnig

    val done =
      Consumer.plainSource(consumerSettings, Subscriptions.topics(topic1))
        .mapAsync(1) { msg =>
          val strMsg = msg.value
          println(s"value: ${strMsg}")
          val tokens = strMsg.split(",")
          if (tokens.size == COLS_NUM) {
            //messaggio consuntivo
            buffer.append(strMsg)
            if (buffer.size == BUFF_SIZE) {
              val path = new Path(HDFS_PATH + "." + new Date().getTime)
              val conf = new Configuration()
              val fs = FileSystem.get(conf)
              val out = fs.create(path)
              val pw = new PrintWriter(out)
              pw.write(header + Properties.lineSeparator)
              buffer.foreach(i=>pw.write(i + Properties.lineSeparator))
              pw.close()
              out.close()
              buffer.clear()
            }
          } else {
            //messaggio preventivo
            println("request prediction for: "+strMsg)
            forecaster ! WineForecasterActor.AskPrediction(strMsg)
          }
          Future.successful(Done)
        }
        .runWith(Sink.ignore)

    done.onComplete(_ => return )

  }

  private def reloadModel() {
  }

}