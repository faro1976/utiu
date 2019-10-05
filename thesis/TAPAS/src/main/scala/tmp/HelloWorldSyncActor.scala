package tmp

import akka.actor.Actor
import akka.actor.Props
import tmp.HelloWorldActor.Greet

//actor ojbect
object HelloWorldSyncActor {
  //instanziazione attore
  def props(): Props =
    Props(new HelloWorldSyncActor())

  //definizione messaggio
  case class Greet(msg: String)
}

//sync actor class
class HelloWorldSyncActor() extends Actor {
  def receive = {
    //ricezione messaggio    
    case Greet(name) =>
      sender ! Greet("Hello " + name + ", Hello world!")
      
      //lookup specifico attore
      context.actorSelection("/user/greeter")
      //loookup con utilizzo di wildcard
      context.actorSelection("/user/greeter*")      
      
  }
}