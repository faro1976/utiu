package tmp

import akka.actor.Actor
import akka.actor.Props
import tmp.HelloWorldActor.Greet

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
  def receive = {
    //ricezione messaggio
    case Greet(name) =>
      println("Hello " + name + ", Hello world!")
  }
}