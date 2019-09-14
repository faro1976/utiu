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
import it.utiu.anavis.BTCPriceTrainerActor
import akka.actor.ActorRef
import it.utiu.tapas.ml.predictor.WineForecasterActor


object WineConsumerActor {

  
  def props(predictor: ActorRef): Props = Props(new WineConsumerActor(predictor))

      
  case class StartConsuming()  

    val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"

}

  class WineConsumerActor(predictor: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {

        case WineConsumerActor.StartConsuming() => doConsuming()
    }
    
    private def doConsuming() {
      
val d1 = List(13.39,1.77,2.62,16.1,90,2.85,2.94,.34,1.45,4.8,.92,3.22,1009)
val d2 = List(12.79,2.67,2.45,22,112,1.48,1.36,.24,1.26,10.8,.48,1.47,344)
val d3 = List(12.15,2.67,2.43,22,112,1.48,1.36,.24,1.26,10.8,.48,1.47,344)

val msgs = List(d1,d2,d3)
      
      predictor ! WineForecasterActor.AskPrediction(msgs)
      
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    import WineConsumerActor._

    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
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

    done.onComplete(_ => return)
      
    }
      
private def reloadModel() {
}

}