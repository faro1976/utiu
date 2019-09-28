package it.utiu.tapas

import java.util.Date

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorRef
import akka.actor.ActorSystem
import it.utiu.anavis.ActivityTrainerActor
import it.utiu.anavis.BTCTrainerActor
import it.utiu.anavis.WineTrainerActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.ml.predictor.ActivityPredictorActor
import it.utiu.tapas.ml.predictor.WinePredictorActor
import it.utiu.tapas.stream.consumer.ActivityConsumerActor
import it.utiu.tapas.stream.consumer.WineConsumerActor
import it.utiu.tapas.stream.producer.ActivityProducerActor
import it.utiu.tapas.stream.producer.WineProducerActor


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
  //define actors
  //wine case study
  var wineTrainerRef: ActorRef = null
  var winePredictorRef: ActorRef = null
  var wineConsumerRef: ActorRef = null
  var wineProducerRef: ActorRef = null
  //activity case study
  var activityTrainerRef: ActorRef = null
  var activityPredictorRef: ActorRef = null
  var activityConsumerRef: ActorRef = null
  var activityProducerRef: ActorRef = null
  //bitcoin case study
  var btcTrainerRef: ActorRef = null

  def run(): Unit = {
    //create wine actors
    wineTrainerRef = system.actorOf(WineTrainerActor.props(), "trainer-wine")
    winePredictorRef = system.actorOf(WinePredictorActor.props(), "predictor-wine")
    wineConsumerRef = system.actorOf(WineConsumerActor.props(winePredictorRef), "consumer-wine")
    wineProducerRef = system.actorOf(WineProducerActor.props(), "producer-wine")
    //create activity actors
    activityTrainerRef = system.actorOf(ActivityTrainerActor.props(), "trainer-activity")
    activityPredictorRef = system.actorOf(ActivityPredictorActor.props(), "predictor-activity")
    activityConsumerRef = system.actorOf(ActivityConsumerActor.props(activityPredictorRef), "consumer-activity")
    activityProducerRef = system.actorOf(ActivityProducerActor.props(), "producer-activity")
    //create bitcoin actors
    btcTrainerRef = system.actorOf(BTCTrainerActor.props(), "btc-activity")

    //start wine actors
    //    wineTrainerRef ! AbstractTrainerActor.StartTraining()
    //    Thread.sleep(2000)
    //    wineConsumerRef ! AbstractConsumerActor.StartConsuming()
    //    Thread.sleep(10000)
    //    wineProducerRef ! AbstractProducerActor.StartProducing()

    //start activity actors
    //    activityTrainerRef ! AbstractTrainerActor.StartTraining()
    //    Thread.sleep(2000)
    //    activityConsumerRef ! AbstractConsumerActor.StartConsuming()
    //    Thread.sleep(10000)
    //    activityProducerRef ! AbstractProducerActor.StartProducing()

    //start bitcoin actors
    btcTrainerRef ! AbstractTrainerActor.StartTraining()

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}

