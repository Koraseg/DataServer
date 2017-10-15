package client

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
/*
simple class for testing
 */
class ClientActor(remote: InetSocketAddress) extends Actor with ActorLogging {
  import context.system

  override def preStart(): Unit = {
    IO(Tcp) ! Connect(remote)
  }

  override def receive: Receive = {
    case Connected(_, _) =>
      val connection = sender()
      connection ! Register(self)
      context.become {
        case Received(data) =>
          log.info(new String(data.toArray))
        case _: ConnectionClosed =>
          log.error("The connection is closed")
          context.stop(self)
          context.system.terminate()
        case msg =>
          log.warning(s"Unexpected message of class ${msg.getClass}")
      }
    case _ =>
  }
}
