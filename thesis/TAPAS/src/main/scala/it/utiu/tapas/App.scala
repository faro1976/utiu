package it.utiu.tapas

import java.util.Date

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.event.Logging
import it.utiu.anavis.WineTrainerActor
import it.utiu.tapas.ml.predictor.WinePredictorActor
import it.utiu.tapas.stream.consumer.WineConsumerActor

/**
 * @author ${user.name}
 */
object App {
  

  
  def main(args: Array[String]) {
    //create akka system
    val system = ActorSystem("tapas")
    val startTime = new Date().getTime    
    val app = new App(system)
    app.run()
    Logging.getLogger(system, this).info("simulations done in "+((new Date().getTime-startTime)/60000)+" mins! Exiting...")
  }

}


class App(system: ActorSystem) {
  var trainerRef: ActorRef = null
  var predictorRef: ActorRef = null
  var consumerRef: ActorRef = null
  var producerRef: ActorRef = null

  def run(): Unit = {
    trainerRef = system.actorOf(WineTrainerActor.props(), "Trainer")
    predictorRef = system.actorOf(WinePredictorActor.props(), "Predictor")
    consumerRef = system.actorOf(WineConsumerActor.props(predictorRef), "Consumer")
    
    trainerRef ! WineTrainerActor.StartTraining()
    consumerRef ! WineConsumerActor.StartConsuming
//    producerRef ! BTCConsumerActor.StartProducer
    Await.ready(system.whenTerminated, Duration.Inf)
  }

}

