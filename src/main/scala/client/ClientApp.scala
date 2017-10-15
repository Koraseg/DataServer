package client

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory


object ClientApp extends App {
  implicit val system = ActorSystem("ClientSystem")
  val config = ConfigFactory.load()
  val connCount = 3
  val serverAddress = new InetSocketAddress(config.getString("app.server.host"), config.getInt("app.server.port"))
  for (_ <- 1 to connCount) {
    system.actorOf(Props(classOf[ClientActor], serverAddress))

  }
}
