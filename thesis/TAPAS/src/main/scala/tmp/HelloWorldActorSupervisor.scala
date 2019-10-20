package tmp

import akka.actor.Actor
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props



class HelloWorldActorSupervisor extends Actor {
 
  override def preStart() = println("The Supervisor is ready to supervise")
  override def postStop() = println("Bye Bye from the Supervisor")
 
  override def supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Resume 
  } 
   
  val printer = context.actorOf(Props(new HelloWorldActor), "greeter")
   
  override def receive: Receive = {
    case msg => printer forward msg
  }
}