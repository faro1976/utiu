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
import it.utiu.tapas.stream.producer.ActivityProducerActor
import it.utiu.tapas.stream.consumer.ActivityConsumerActor
import it.utiu.tapas.ml.predictor.ActivityPredictorActor
import it.utiu.anavis.ActivityTrainerActor


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
  var wineTrainerRef: ActorRef = null
  var winePredictorRef: ActorRef = null
  var wineConsumerRef: ActorRef = null
  var wineProducerRef: ActorRef = null
  
  var activityTrainerRef: ActorRef = null
  var activityPredictorRef: ActorRef = null
  var activityConsumerRef: ActorRef = null
  var activityProducerRef: ActorRef = null
  

  def run(): Unit = {
    wineTrainerRef = system.actorOf(WineTrainerActor.props(), "trainer-wine")
    winePredictorRef = system.actorOf(WinePredictorActor.props(), "predictor-wine")
    wineConsumerRef = system.actorOf(WineConsumerActor.props(winePredictorRef), "consumer-wine")
    wineProducerRef = system.actorOf(WineProducerActor.props(), "producer-wine")

    activityTrainerRef = system.actorOf(ActivityTrainerActor.props(), "trainer-activity")
    activityPredictorRef = system.actorOf(ActivityPredictorActor.props(), "predictor-activity")
    activityConsumerRef = system.actorOf(ActivityConsumerActor.props(activityPredictorRef), "consumer-activity")
    activityProducerRef = system.actorOf(ActivityProducerActor.props(), "producer-activity")
    
//    wineTrainerRef ! AbstractTrainerActor.StartTraining()
//    Thread.sleep(2000)
//    wineConsumerRef ! AbstractConsumerActor.StartConsuming()
//    Thread.sleep(10000)
//    wineProducerRef ! AbstractProducerActor.StartProducing()

    activityTrainerRef ! AbstractTrainerActor.StartTraining()
    Thread.sleep(2000)
    activityConsumerRef ! AbstractConsumerActor.StartConsuming()
    Thread.sleep(10000)
    activityProducerRef ! AbstractProducerActor.StartProducing()    
    
    Await.ready(system.whenTerminated, Duration.Inf)
  }

}

