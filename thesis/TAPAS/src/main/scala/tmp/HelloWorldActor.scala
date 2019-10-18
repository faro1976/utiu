package tmp

import akka.actor.Actor
import akka.actor.Props
import tmp.HelloWorldActor.Greet
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import tmp.Exceptions3.ResumeException3
import tmp.Exceptions3.RestartException3





//actor ojbect
object HelloWorldActor {
  //instanziazione attore
  def props(): Props =
    Props(new HelloWorldActor())

  //definizione messaggio
  case class Greet(msg: String)
}

//actor class
class HelloWorldActor() extends Actor {
  var i = 1
  
 override def preRestart(reason: Throwable, message: Option[Any]) = {
    println("preRestart...")
    super.preRestart(reason, message)
  }
 
  override def postRestart(reason: Throwable) = {
    println("...postRestart")
    super.postRestart(reason)
  }
   
  override def preStart() = println("preStart")
  override def postStop() = println("postStop")
  
  override val supervisorStrategy =
    OneForOneStrategy() {
      case ResumeException3 => Stop
      case RestartException3 => Stop
      case _: Exception => Escalate
    }
  
  def receive = {    
    //ricezione messaggio
    case Greet(name) =>
      if (i%3==0) throw RestartException3 else{println(i)
        i=i+1}
      println("Hello " + name + ", Hello world!")
  }
}

