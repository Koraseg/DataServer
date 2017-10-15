package server

import java.util

import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.{ByteString, ByteStringBuilder}
import org.joda.time.DateTime
import scala.collection.mutable
import scala.collection.JavaConversions._

object ServiceActor {
  val maxArchiveSize = 60
}

class ServiceActor(workersCount: Int) extends Actor with ActorLogging {
  import context.dispatcher
  import ServiceActor._

  type CandleStick = ByteString
  private[this] var workers: IndexedSeq[ActorRef] = _
  private[this] var trackingMap: mutable.Map[Int, Boolean] = (0 until workersCount).map(i => i -> false)(scala.collection.breakOut)
  private[this] val candleResponseBuilder: ByteStringBuilder = ByteString.newBuilder
  private[this] var candleTime: DateTime =_
  private[this] val archive = new util.ArrayDeque[CandleStick]()



  override def preStart(): Unit = {
    val now = DateTime.now()
    candleTime = now.minuteOfDay().roundCeilingCopy()
    workers = (0 until workersCount).map(i => context.actorOf(Props(classOf[WorkerActor], i, candleTime)))
    context.system.scheduler.schedule(candleTime.minus(now.getMillis).getMillis millis, 1 minute, self, CandleTimeUpdate)
  }

  override def receive: Receive = {
    case raw @ DealInputRaw(bs) =>
      //determine worker to process this data
      workers(routing(bs)) ! raw

    case CandleTimeUpdate =>
      log.debug("Force workers to send aggregated data")
      workers.foreach(_ ! BuildCandleRequest(candleTime))
      candleTime = candleTime.plusMinutes(1)

    case CandleArchiveRequest(itemsCount) =>
      sender() ! CandleArchiveResponse(archive.take(itemsCount))

    case PieceOfCandle(id, candlePack) =>
      log.debug(s"Got response from $id worker")
      trackingMap(id) = true
      candleResponseBuilder.append(candlePack)
      if (trackingMap.forall(_._2)) {
        log.debug("Publishing new candle")
        val newCandle = candleResponseBuilder.result()
        context.system.eventStream.publish(CandleRawData(newCandle))
        updateInnerState(newCandle)
      }

    case msg =>
      log.warning(s"Unexpected message of class  ${msg.getClass}")
  }



  /*in the nutshell it is just a calculation of ticker hashcode
    to ensure that data with equal tickers goes to same worker
   */

  @inline
  private def routing(bs: ByteString): Int = {

    val tickerLen = (bs(10) & 0xff) << 8 | (bs(11) & 0xff) << 0
    var i = 0
    var byteHash = 0
    while (i < tickerLen) {
      byteHash = byteHash * 37 + bs(12 + i)
      i += 1
    }
    byteHash % workersCount
  }

  private def updateInnerState(candle: CandleStick): Unit = {
    archive.addFirst(candle)
    if (archive.size() > maxArchiveSize) {
      archive.removeLast()
    }
    candleResponseBuilder.clear()
    trackingMap = (0 until workersCount).map(i => i -> false)(scala.collection.breakOut)
  }

}
