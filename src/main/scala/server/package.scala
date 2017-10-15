
import java.nio.ByteOrder

import akka.util.ByteString
import org.joda.time.DateTime


package object server {

  final case class DealInputRaw(bs: ByteString)
  final case class CandleArchiveRequest(itemsCount: Int)
  final case class PieceOfCandle(workerId: Int, candlePack: ByteString)
  final case class BuildCandleRequest(ts: DateTime)
  final case class CandleRawData(bs: ByteString)
  final case class CandleArchiveResponse(candles: Iterable[ByteString])
  case object CandleTimeUpdate

  object DealInput {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

    @inline
    def apply(bs: ByteString): DealInput = {
      val it = bs.iterator
      //ignore total length
      val time = new DateTime(it.drop(2).getLong)
      val tickerLen = it.getShort
      val ticker = new String(it.getBytes(tickerLen))
      val price = it.getDouble
      val size = it.getInt
      new DealInput(time, ticker, price, size)
    }

  }

  final case class DealInput(time: DateTime, ticker: String, price: Double, size: Int)

  object CandleDataAccumulator {
    def apply(fInp: DealInput): CandleDataAccumulator = {
      CandleDataAccumulator(open = fInp.price, high = fInp.price, low = fInp.price, close = fInp.price,
        volume = fInp.size)
    }

    def fromOpen(open: Double): CandleDataAccumulator = CandleDataAccumulator(
      open = open,
      high = open,
      low = open,
      close = open,
      volume = 0
    )
  }

  //assuming messages are chronologically ordered because upstream maintains this order
  final case class CandleDataAccumulator(
    var open: Double,
    var high: Double,
    var low: Double,
    var close: Double,
    var volume: Int
  ) {

    def +=(fInput: DealInput): this.type = {
      close = fInput.price
      high = math.max(high, fInput.price)
      low = math.min(low, fInput.price)
      volume += fInput.size
      this
    }

    def enrichWithOpen(open: Double): this.type = {
      this.open = open
      this.high = math.max(open, high)
      this.low = math.min(open, low)
      this
    }


  }

}
