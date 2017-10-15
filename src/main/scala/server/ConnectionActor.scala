package server

import java.net.InetSocketAddress

import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.{IO, Tcp}
import akka.io.Tcp.{Closed, _}
import akka.util.ByteString

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ConnectionActor(address: InetSocketAddress, master: ActorRef) extends Actor with ActorLogging {
  import context.system
  import context.dispatcher

  override def preStart(): Unit = {
    IO(Tcp) ! Bind(self, address)
  }

  override def receive: Receive = {
    case Bound(_) =>
      context.become {
        case Connected(_, _) =>
          val connection = sender()
          val handler = context.actorOf(Props(classOf[HandlerActor], connection, master))
          connection ! Register(handler)
        case msg =>
          log.warning(s"Unexpected message of class ${msg.getClass}")
      }
    case CommandFailed(_) =>
      log.error("Could not bind to the specified address. Terminataion")
      context.system.terminate()
  }
}


class HandlerActor(conn: ActorRef, service: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher


  override def preStart(): Unit = {
    service ! CandleArchiveRequest(10)
  }

  override def receive: Receive = {
    case CandleArchiveResponse(candles) =>
      context.system.eventStream.subscribe(self, classOf[CandleRawData])
      conn ! Write(candles.fold(ByteString.empty)(_ ++ _))
    case CandleRawData(bs) =>
      conn ! Write(bs)
    case _: ConnectionClosed =>
      log.debug(s"Client connection has been closed. Stop this handler.")
      context stop self
    case msg =>
      log.warning(s"Handler got an unexpected message of class ${msg.getClass}")
  }
}
