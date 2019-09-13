package it.utiu.tapas

import scala.concurrent.Await
import akka.actor.ActorSystem
import akka.event.Logging
import java.util.Date
import akka.actor.ActorRef
import scala.concurrent.duration.Duration
import it.utiu.tapas.stream.consumer.BTCConsumerActor
import it.utiu.anavis.DummyTrainerActor
import it.utiu.tapas.stream.consumer.DummyConsumerActor
import it.utiu.tapas.ml.predictor.DummyPredictorActor

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
    trainerRef = system.actorOf(DummyTrainerActor.props(), "Trainer")
    predictorRef = system.actorOf(DummyPredictorActor.props(), "Predictor")
    consumerRef = system.actorOf(DummyConsumerActor.props(predictorRef), "Consumer")
    
    trainerRef ! DummyTrainerActor.StartTraining()
//    consumerRef ! BTCConsumerActor.StartConsuming
//    producerRef ! BTCConsumerActor.StartProducer
    Await.ready(system.whenTerminated, Duration.Inf)
  }

}

