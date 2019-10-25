package it.utiu.tapas

import java.util.Date

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorRef
import akka.actor.ActorSystem
import it.utiu.anavis.ActivityTrainerActor
import it.utiu.anavis.BTCTrainerActor
import it.utiu.anavis.WineTrainerActor
import it.utiu.tapas.base.AbstractAnalyzerActor
import it.utiu.tapas.base.AbstractTrainerActor
import it.utiu.tapas.ml.predictor.ActivityPredictorActor
import it.utiu.tapas.ml.predictor.WinePredictorActor
import it.utiu.tapas.stream.consumer.AbstractConsumerActor
import it.utiu.tapas.stream.consumer.AbstractProducerActor
import it.utiu.tapas.stream.consumer.ActivityConsumerActor
import it.utiu.tapas.stream.consumer.BTCConsumerActor
import it.utiu.tapas.stream.consumer.WineConsumerActor
import it.utiu.tapas.stream.producer.ActivityProducerActor
import it.utiu.tapas.stream.producer.BTCProducerActor
import it.utiu.tapas.stream.producer.WineProducerActor
import it.utiu.tapas.util.Consts.CS_ACTIVITY
import it.utiu.tapas.util.Consts.CS_BTC
import it.utiu.tapas.util.Consts.CS_WINE
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import akka.Version
import it.utiu.tapas.stats.analyzer.BTCAnalyzerActor
import it.utiu.tapas.ml.predictor.BTCPredictorActor
import it.utiu.tapas.ml.predictor.BTCFeederActor
import akka.actor.Cancellable
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object Runner {

  def main(args: Array[String]) {
    //create akka system
    val system = ActorSystem("tapas")
    println("starting TAPAS at " + new Date() + "...")
    val cs = if (args.size > 0) args(0) else CS_BTC //default case study
    val app = new Runner(system, cs)
    app.run(cs)
  }
}

class Runner(system: ActorSystem, cs: String) {
  //define actors
  var trainerRef: ActorRef = null
  var predictorRef: ActorRef = null
  var consumerRef: ActorRef = null
  var producerRef: ActorRef = null
  var analyzerRef: ActorRef = null
  var feederRef: ActorRef = null

  def run(cs: String): Unit = {
    //create actors
    cs match {
      case CS_ACTIVITY =>
        //create activity actors
        trainerRef = system.actorOf(ActivityTrainerActor.props(), "trainer-activity")
        predictorRef = system.actorOf(ActivityPredictorActor.props(), "predictor-activity")
        consumerRef = system.actorOf(ActivityConsumerActor.props(), "consumer-activity")
        producerRef = system.actorOf(ActivityProducerActor.props(), "producer-activity")
        feederRef = system.actorOf(ActivityProducerActor.props(), "feeder-activity")
      case CS_BTC =>
        trainerRef = system.actorOf(BTCTrainerActor.props(), "trainer-btc")
        predictorRef = system.actorOf(BTCPredictorActor.props(), "predictor-btc")
        feederRef = system.actorOf(BTCFeederActor.props(), "feeder-btc")
        analyzerRef = system.actorOf(BTCAnalyzerActor.props(), "analyzer-btc")
        consumerRef = system.actorOf(BTCConsumerActor.props(), "consumer-btc")
        producerRef = system.actorOf(BTCProducerActor.props(), "producer-btc")
      case CS_WINE =>
        //create wine actors
        trainerRef = system.actorOf(WineTrainerActor.props(), "trainer-wine")
        predictorRef = system.actorOf(WinePredictorActor.props(), "predictor-wine")
        consumerRef = system.actorOf(WineConsumerActor.props(), "consumer-wine")
        producerRef = system.actorOf(WineProducerActor.props(), "producer-wine")
      case _ => throw new RuntimeException(cs + " case study code not supported!")
    }

    //start actors
    Thread.sleep(2000)
    //consumer
    consumerRef ! AbstractConsumerActor.StartConsuming()
    Thread.sleep(2000)
    //producer
    producerRef ! AbstractProducerActor.StartProducing()
    Thread.sleep(2000)
    system.scheduler.scheduleOnce(1 minute) {
      //trainer
      trainerRef ! AbstractTrainerActor.StartTraining()
    }
    //stats data feeder (if supported)
    if (analyzerRef != null) {
      system.scheduler.scheduleOnce(10 minute) {
        analyzerRef ! AbstractAnalyzerActor.StartAnalysis()
      }
    }

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}

