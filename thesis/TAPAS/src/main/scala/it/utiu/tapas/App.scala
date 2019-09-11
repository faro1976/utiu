package it.utiu.tapas

import scala.concurrent.Await
import akka.actor.ActorSystem
import akka.event.Logging
import java.util.Date
import akka.actor.ActorRef
import it.utiu.anavis.BTCPriceTrainerActor
import scala.concurrent.duration.Duration
import it.utiu.tapas.stream.consumer.BTCConsumerActor

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
  var consumerRef: ActorRef = null

  def run(): Unit = {
    trainerRef = system.actorOf(BTCPriceTrainerActor.props(), "BTCTrainer")
    consumerRef = system.actorOf(BTCConsumerActor.props(), "BTCConsumer")
    trainerRef ! BTCPriceTrainerActor.StartTraining
    consumerRef ! BTCConsumerActor.StartConsuming
    Await.ready(system.whenTerminated, Duration.Inf)
  }

}

