package it.utiu.tapas

import java.util.Date
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.event.Logging
import it.utiu.anavis.WineTrainerActor
import it.utiu.tapas.stream.consumer.WineConsumerActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.ml.predictor.WinePredictorActor
import it.utiu.tapas.stream.producer.WineProducerActor
import it.utiu.tapas.stream.consumer.AbstractProducerActor
import it.utiu.tapas.stream.consumer.AbstractConsumerActor


/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    //create akka system
    val system = ActorSystem("tapas")
    println("starting TAPAS at " + new Date() + "...")
    val app = new App(system)
    app.run()
  }

}

class App(system: ActorSystem) {
  var trainerRef: ActorRef = null
  var predictorRef: ActorRef = null
  var consumerRef: ActorRef = null
  var producerRef: ActorRef = null

  def run(): Unit = {
    trainerRef = system.actorOf(WineTrainerActor.props(), "trainer")
    predictorRef = system.actorOf(WinePredictorActor.props(), "predictor")
    consumerRef = system.actorOf(WineConsumerActor.props(predictorRef), "consumer")
    producerRef = system.actorOf(WineProducerActor.props(), "producer")

//    trainerRef ! AbstractTrainerActor.StartTraining()
    Thread.sleep(3000)
    consumerRef ! AbstractConsumerActor.StartConsuming()
    Thread.sleep(10000)
    producerRef ! AbstractProducerActor.StartProducing()
    Await.ready(system.whenTerminated, Duration.Inf)
  }

}

