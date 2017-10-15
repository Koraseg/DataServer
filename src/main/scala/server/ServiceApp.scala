package server

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object ServiceApp extends App {
    implicit val system: ActorSystem = ActorSystem("ServiceActorSystem")

    try {
      val config = ConfigFactory.load()
      val upstreamAddress = new InetSocketAddress(config.getString("app.upstream.host"), config.getInt("app.upstream.port"))
      val serverAddress = new InetSocketAddress(config.getString("app.server.host"), config.getInt("app.server.port"))
      val workersCount = config.getInt("app.workers-count")

      //starting main actor
      system.actorOf(Props(classOf[MainActor], upstreamAddress, serverAddress, workersCount))
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        system.terminate()
    }
}
