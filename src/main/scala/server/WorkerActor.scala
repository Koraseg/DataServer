package server

import akka.actor.{Actor, ActorLogging}
import akka.util.ByteString
import org.joda.time.DateTime
import org.json4s.native.JsonMethods.{compact, render}
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.collection.mutable

class WorkerActor(id: Int, startCandleTime: DateTime) extends Actor with ActorLogging {
  type Ticker = String
  type OneCandleData = mutable.Map[Ticker, CandleDataAccumulator]

  private val newLineBytes = "\n".getBytes()

  private[this] var currentCandle = startCandleTime
  private[this] var currentCandleAccum: OneCandleData = mutable.Map()

  //need to have this one in case the data from next candle arrives before candle request
  private[this] var nextCandleAccum: OneCandleData = mutable.Map()

  override def receive: Receive = {
    case DealInputRaw(bs) =>
      val f = DealInput(bs)
      val ts = f.time.minuteOfDay().roundCeilingCopy()
      if (ts == currentCandle) {
        val newAcc = currentCandleAccum.get(f.ticker).map(_ += f).getOrElse(CandleDataAccumulator(f))
        currentCandleAccum(f.ticker) = newAcc
      } else {
        val newAcc = nextCandleAccum.get(f.ticker).map(_ += f).getOrElse(CandleDataAccumulator(f))
        nextCandleAccum(f.ticker) = newAcc
      }

    case BuildCandleRequest(ts) =>
      assert(ts == currentCandle)
      log.debug(s"Serialization request for ts $ts in ${DateTime.now()}")
      val candlePack = serialize(ts)
      sender() ! PieceOfCandle(id, candlePack)
      currentCandle = currentCandle.plusMinutes(1)

      //need to set appropriate open prices for next candle
      for ((ticker, currentAcc) <- currentCandleAccum if currentAcc.volume > 0) {
        nextCandleAccum.get(ticker) match {
          //set open price
          case Some(nextAcc) =>
            nextAcc.enrichWithOpen(currentAcc.close)
          case None =>
            nextCandleAccum(ticker) = CandleDataAccumulator.fromOpen(currentAcc.close)
        }
      }

      currentCandleAccum = nextCandleAccum
      nextCandleAccum = mutable.Map()

    case any =>
      log.error(any.toString)
  }




  private def serialize(dt: DateTime): ByteString = {
    val byteStringBuilder = ByteString.newBuilder
    for ((ticker, acc)  <- currentCandleAccum if acc.volume > 0) {
      val oneJson =  ("ticker" -> ticker) ~
        ("timestamp" -> dt.toString) ~
        ("open" -> acc.open) ~
        ("high" -> acc.high) ~
        ("low" -> acc.low) ~
        ("close" -> acc.close) ~
        ("volume" -> acc.volume)
      byteStringBuilder.putBytes(compact(render(oneJson)).getBytes())
      byteStringBuilder.putBytes(newLineBytes)
    }
    byteStringBuilder.result()
  }


}
