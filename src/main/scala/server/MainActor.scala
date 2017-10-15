package server

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.io.Tcp._
import com.typesafe.config.ConfigFactory

class MainActor(upstream: InetSocketAddress, serverAddress: InetSocketAddress, workersCount: Int) extends Actor with ActorLogging {
  import context.system

  val serviceActor: ActorRef = context.actorOf(Props(classOf[ServiceActor], workersCount))
  val connectionActor: ActorRef = context.actorOf(Props(classOf[ConnectionActor], serverAddress, serviceActor))

  override def preStart(): Unit = {
    IO(Tcp) ! Connect(upstream)
  }

  override def receive: Receive = {
    case CommandFailed(_) =>
      log.error("Could not connect to the upstream. Application is shutting down...")
      context.system.terminate()
    case Connected(_, _) =>
      val connection = sender()
      connection ! Register(self)

      context.become {
        case Received(data) =>
          serviceActor ! DealInputRaw(data)
        case _: ConnectionClosed =>
          log.error("The connection is closed!!!")
          context.system.terminate()
        case msg =>
          log.warning(s"Unexpected message of class ${msg.getClass}")
      }
  }
}

