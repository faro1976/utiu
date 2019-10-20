package tmp

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import akka.actor.ActorSystem
import tmp.HelloWorldActor.Greet
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object App {
  def main(args: Array[String]) {
    //creazione Akka system
    val system = ActorSystem("hello-world")
    //creazione attore HelloWorld
    val greeter = system.actorOf(HelloWorldActor.props(),  "greeter")
    
    while(true){
    //invio messaggio fire-and-forget
    greeter ! Greet("Rob")
    Thread.sleep(5000)
    }
    val syncGreeter = system.actorOf(HelloWorldSyncActor.props(),  "syncGreeter")
    implicit val timeout = Timeout(1 second)
    //invio messaggio request/response
    val future = syncGreeter ? Greet("Rob2")
    val result = Await.result(future, timeout.duration)
    println("sync response: " + result)
        
    Await.ready(system.whenTerminated, Duration.Inf)
  }
}

