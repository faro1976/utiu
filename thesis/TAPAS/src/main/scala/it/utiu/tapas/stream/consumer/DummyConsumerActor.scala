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
import it.utiu.tapas.ml.predictor.DummyPredictorActor


object DummyConsumerActor {

  
  def props(predictor: ActorRef): Props = Props(new DummyConsumerActor(predictor))

      
  case class StartConsuming()  

    val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"

}

  class DummyConsumerActor(predictor: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {

        case DummyConsumerActor.StartConsuming() => doConsuming()
    }
    
    private def doConsuming() {
      
val d1 = List(1l,2l,3l,4l,5l,6l,7l)
val d2 = List(1l,2l,3l,4l,5l,6l,7l)
val d3 = List(1l,2l,3l,4l,5l,6l,7l)
val d4 = List(1l,2l,3l,4l,5l,6l,7l)
val d5 = List(1l,2l,3l,4l,5l,6l,7l)

val msgs = List(d1,d2,d3,d4,d5)
      
      predictor ! DummyPredictorActor.AskPrediction(msgs)
      
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    import DummyConsumerActor._

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