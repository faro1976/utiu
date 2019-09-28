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
import it.utiu.anavis.BTCTrainerActor
import it.utiu.tapas.base.AbstractTrainerActor


object BTCConsumerActor {

  
  def props(): Props = Props(new BTCConsumerActor())

      
  case class StartConsuming()  

    val topic1 = "test"
  val kafkaBootstrapServers = "localhost"
  val groupId = "group1"

}

  class BTCConsumerActor extends Actor with ActorLogging {
    override def receive: Receive = {

        case AbstractConsumerActor.StartConsuming() => doConsuming()
        case AbstractTrainerActor.TrainingFinished => reloadModel()
    }
    
    private def doConsuming() {
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContext = context.system.dispatcher
    import BTCConsumerActor._

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