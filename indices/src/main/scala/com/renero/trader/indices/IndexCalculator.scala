package com.renero.trader.indices

import com.github.nscala_time.time.Imports._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class TickData(day: DateTime, hour: DateTime, price: Double, volume: Double, low: Double, high:Double)
case class TimeSerie(day: DateTime, value: Double)

object IndexCalculator {

  val CurrentDate = new DateTime(2013, 12, 31, 0, 0, 0, 0).getMillis

  def process(sc: SparkContext, path: String) {
    val tickData = sc.textFile(path).map { line =>
      val fields = line.split(',')
      new TickData(
        DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(fields(0).substring(0, 10)),
        DateTimeFormat.forPattern("HH:mm:ss").parseDateTime(fields(0).substring(11)),
        fields(1).toDouble, fields(2).toDouble, 0, 0
      )
    }

    val close = closeValues(tickData)

    val PVI = computeVolumeIndex(close, (today, yesterday) => today < yesterday)
    val NVI = computeVolumeIndex(close, (today, yesterday) => today >= yesterday)
    val sPVI = exponentialAverage(PVI)
    val sNVI = exponentialAverage(NVI)
    val (pvimax, pvimin) = highestLowest(sPVI, CurrentDate, sPVI.size)
    val (nvimax, nvimin) = highestLowest(sNVI, CurrentDate, sNVI.size)

    val oscp = computeOSCP(PVI, sPVI, pvimax.value, pvimin.value)
    val azul = computeOSCP(NVI, sNVI, nvimax.value, nvimin.value)

    val MFI = moneyFlowIndex(close)
    val bollingerOsc = bollingerOscillator(close)
    val RSI = relativeStrengthIndex(close)
    val STOC = stochasticOscillator(close.toList, 14, 3)

    val firstDay = new DateTime(2013, 2, 5, 0, 0, 0, 0).getMillis
    val brown = computeBrown(RSI, MFI, bollingerOsc, STOC, firstDay)
    val green = computeGreen(brown, oscp, firstDay)
  }

  /**
   * Compute the values of the ticks when the session closes.
   * @param tickData  the series of tickData
   * @return          the values of the tick at close
   */
  def closeValues(tickData: RDD[TickData]): Array[TickData] = {
    val midnight = new DateTime(1970, 1, 1, 23, 59, 59, 0)
    val ticksGroupedByDate = tickData.groupBy(_.day).sortBy { case (day, _) => day }
    ticksGroupedByDate.collect.map { pair =>
      val (day, ticks) = pair
      val openPrice = ticks.head.price
      val closePrice = ticks.last.price
      val closeVolume = ticks.map(_.volume).sum
      val low  = ticks.foldLeft(openPrice)((min, tick) => Math.min(tick.price, min))
      val high = ticks.foldLeft(openPrice)((max, tick) => Math.max(tick.price, max))
      TickData(day, midnight, closePrice, closeVolume, low, high)
    }
  }

  /**
   * Compute the IVN/P (N/P Volume Index) index:
   * https://www.visualchart.com/esxx/ayuda_F1/Indicators/Volumen/IVN.htm
   * http://www.metastock.com/Customer/Resources/TAAZ/?c=3&p=92
   * @param data      The data over which computing the INV index
   * @return          A list with the index, computed for evey change in Tick value.
   */
  def computeVolumeIndex(data: Array[TickData], matchesVolumeIndexLambda: (Double,Double) => Boolean)
      : List[TimeSerie] = {
    val initValue = TimeSerie(data(0).day, 1000.0)
    data.sliding(2).foldLeft(List[TimeSerie](initValue)) { (index, pair) =>
      if (matchesVolumeIndexLambda(pair(1).volume, pair(0).volume))
        index :+ TimeSerie(pair(1).day, index.reverse.head.value)
      else {
        val yesterdaysVI = index.reverse.head.value
        def percentagePriceChange(pair: Array[TickData]): Double =
          ((pair(1).price - pair(0).price) / pair(0).price) * 100
        index :+ TimeSerie(pair(1).day, yesterdaysVI + percentagePriceChange(pair))
      }
    }
  }

  /**
   * compute Single Exponent Weighted Smoothing to a series of data. Simple method.
   * @param data      the series of index data (date, value)
   * @param alpha     the smoothing parameter (the shorter, the smoother).
   * @return          a list with the smoothed values
   */
  def exponentialAverage(data: List[TimeSerie], alpha: Double = 0.1): List[TimeSerie] =
    data.foldLeft(List[TimeSerie](data(0))) { (S, Y) =>
      S :+ TimeSerie(Y.day, alpha * Y.value + (1 - alpha) * S.reverse.head.value)
    }.tail

  def prints(serie: List[TimeSerie]) {
    serie.foreach(ts => println(ts.value))
  }

  def computeOSCP(volIndex: List[TimeSerie], sVolIndex: List[TimeSerie], viMax: Double, viMin: Double)
      : List[TimeSerie] =
    volIndex.zip(sVolIndex).map {
      case (vi, expVI) => TimeSerie(vi.day, ((vi.value-expVI.value) * 100) / (viMax - viMin))
    }

  /* *** Money Flow Index *** */
  def computeTypical(data: Array[TickData]): Array[TimeSerie] =
    data.map(tick => TimeSerie(tick.day, (tick.price * tick.high * tick.low) / 3))

  def computeRawMoneyFlow(ticks: Array[TickData], typical: Array[TimeSerie]): Array[TimeSerie] = {
    val volume = ticks.map(_.volume)
    val rawMoneyFlow = typical.zip(volume).map {
      case (typData, vol) => TimeSerie(typData.day, typData.value * vol)
    }
    val initValue = TimeSerie(ticks(0).day, 1)
    val upDownList = ticks.sliding(2).foldLeft(List[TimeSerie](initValue)) { (upOrDownList, pair) =>
      if (pair(1).price >= pair(0).price) upOrDownList :+ TimeSerie(pair(1).day, 1)
      else upOrDownList :+ TimeSerie(pair(1).day, -1)
    }.tail
    rawMoneyFlow.tail.zip(upDownList).map { case (raw, sign) => TimeSerie(raw.day, raw.value * sign.value) }
  }

  def computeMoneyFlowRatio(rawMoneyFlow: Array[TimeSerie]): List[TimeSerie] =
    rawMoneyFlow.sliding(14).foldLeft(List[TimeSerie]()) { (mfrList, windowMF) =>
      val initValue = 0.0
      val (positives, negatives) = windowMF.foldLeft(z = (initValue, initValue)) { (z, mf) =>
        if (mf.value >= 0) (z._1 + mf.value, z._2) else (z._1, z._2 + mf.value)
      }
      mfrList :+ TimeSerie(windowMF.last.day, Math.abs(positives / negatives))
    }

  def moneyFlowIndex(close: Array[TickData]): List[TimeSerie] = {
    val typical = computeTypical(close)
    val rawMoneyFlow = computeRawMoneyFlow(close, typical)
    val moneyFlowRatio = computeMoneyFlowRatio(rawMoneyFlow)
    moneyFlowRatio.map(x => TimeSerie(x.day, 100 - (100 / (1 + x.value))))
  }

  /**
   * Compute the highest and lowest value in a series of Index Data (date, value) pairs
   * @param indices       The indices to search the max and min over.
   * @param currentDate   The later date for which the max/min are to be searched for.
   * @param windowSize    The number of days to look for the max/min backwards.
   */
  def highestLowest(indices: List[TimeSerie], currentDate: Long, windowSize: Int): (TimeSerie, TimeSerie) = {
    val windowValues = indices.filter(_.day.getMillis <= currentDate).reverseIterator.take(windowSize).toList
    val max = windowValues.foldLeft(indices(0)) ((max, x) => if (x.value > max.value) x else max)
    val min = windowValues.foldLeft(indices(0)) ((min, x) => if (x.value < min.value) x else min)
    (max, min)
  }

  def highest(indices: List[TimeSerie], currentDate: Long, windowSize: Int): TimeSerie = {
    val windowValues = indices.filter(_.day.getMillis <= currentDate).reverseIterator.take(windowSize).toList
    windowValues.foldLeft(indices(0)) ((max, x) => if (x.value > max.value) x else max)
  }

  def lowest(indices: List[TimeSerie], currentDate: Long, windowSize: Int): TimeSerie = {
    val windowValues = indices.filter(_.day.getMillis <= currentDate ).reverseIterator.take(windowSize).toList
    windowValues.foldLeft(indices(0)) ((min, x) => if (x.value < min.value) x else min)
  }

  def mean(xs: List[Double]): Double = if (xs.isEmpty) 0.0 else xs.sum / xs.size

  def stddev(xs: List[Double], avg: Double): Double =
    if (xs.isEmpty) 0.0 else math.sqrt((0.0 /: xs) { (a, e) => a + math.pow(e - avg, 2.0) } / xs.size)

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_bands
   */
  def bollingerBands(data: List[TickData], windowSize: Int  =25): (List[TimeSerie], List[TimeSerie]) = {
    data.sliding(windowSize).foldLeft(List[(TimeSerie, TimeSerie)]()) { (bollinger, windowTicks) =>
      val priceList = windowTicks.map(tick => TimeSerie(tick.day, tick.price))
      val priceListValues = priceList.map(_.value)
      val sma = mean(priceListValues)
      val stdev = stddev(priceListValues, sma)
      bollinger :+(TimeSerie(priceList.last.day, sma + (stdev * 2)), TimeSerie(priceList.last.day, sma - (stdev * 2)))
    }.unzip
  }
  
  def bollingerAverage(bUp: List[TimeSerie], bDown: List[TimeSerie]): List[TimeSerie] = bUp.zip(bDown).map {
    case (up, down) => TimeSerie(up.day, (up.value + down.value) / 2)
  }

  def bollingerRange(bUp: List[TimeSerie], bDown: List[TimeSerie]): List[TimeSerie] = bUp.zip(bDown).map {
    case (up, down) => TimeSerie(up.day, up.value - down.value)
  }

  /**
   * OB1 = (BollingerUp[25](TotalPrice) + BollingerDown[25](TotalPrice)) / 2
   * OB2 = BollingerUp[25](TotalPrice) - BollingerDown[25](TotalPrice)
   * BollOsc = ((TotalPrice - OB1) / OB2 ) * 100
   */
  def bollingerOscillator(ticks:Array[TickData]): List[TimeSerie] = {
    val (bUp, bDown) = bollingerBands(ticks.toList)
    val bAvg = bollingerAverage(bUp, bDown)
    val bRng = bollingerRange(bUp, bDown)
    val validTicks = ticks.drop(ticks.size - bAvg.size).map(_.price)
    (validTicks, bAvg, bRng).zipped.toList.map {
      case (price, avg, rng) => TimeSerie(avg.day, ((price - avg.value) / rng.value) * 100)
    }
  }

  /** RSI **/
  def positive(ts:TimeSerie): Boolean = ts.value >= 0
  def negative(ts:TimeSerie): Boolean = !positive(ts)
  def avgGainLoss(wSize: Int, gainLoss: List[TimeSerie], firstAvg: TimeSerie, criteria: TimeSerie => Boolean)
      : List[TimeSerie] =
    gainLoss.drop(wSize).foldLeft(List[TimeSerie](firstAvg)) { (avgLossList, current) =>
      if (criteria(current)) avgLossList :+ TimeSerie(
        current.day,
        Math.abs(((avgLossList.reverse.head.value * (wSize - 1)) + Math.abs(current.value)) / wSize)
      )
      else avgLossList :+ TimeSerie(current.day, Math.abs(avgLossList.reverse.head.value * (wSize - 1) / wSize))
    }

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
   * @param close
   * @param windowSize
   * @return
   */
  def relativeStrengthIndex(close: Array[TickData], windowSize:Int = 14): List[TimeSerie] = {
    val gainLoss = close.sliding(2).foldLeft(List[TimeSerie]()) { (gainLossList, pair) =>
      gainLossList :+ TimeSerie(pair(1).day, pair(1).price - pair(0).price)
    }
    val firstRSIDay = gainLoss.take(windowSize + 1).reverse.head.day
    val firstAvgGain = TimeSerie(
      firstRSIDay,
      gainLoss.take(windowSize).collect { case ts if ts.value > 0 => ts.value }.sum / windowSize
    )
    val firstAvgLoss = TimeSerie(
      firstRSIDay,
      Math.abs(gainLoss.take(windowSize).collect { case ts if ts.value < 0 => ts.value }.sum / windowSize)
    )
    val avgGain = avgGainLoss(windowSize, gainLoss, firstAvgGain, positive)
    val avgLoss = avgGainLoss(windowSize, gainLoss, firstAvgLoss, negative)
    avgGain.zip(avgLoss).map {
      case (gain, loss) =>
        if (loss.value != 0) TimeSerie(gain.day, 100 - (100 / (1 + (gain.value / loss.value))))
        else TimeSerie(gain.day, 100)
    }
  }

  def takeHigh(tick: TickData): Double = tick.high
  def takeLow(tick: TickData): Double = tick.low
  def takePrice(tick: TickData): Double = tick.price
  def takeVolume(tick: TickData): Double = tick.volume
  def tsFromTick(ticks:List[TickData], selector:TickData => Double): List[TimeSerie] =
    ticks.map(tick => TimeSerie(tick.day, selector(tick)))

  def stochasticOscillator(close: List[TickData], dayPeriod: Int, periodMA: Int): List[TimeSerie] =
    close.sliding(dayPeriod).map { period =>
      val highestHigh = highest(tsFromTick(period, takeHigh), period.last.day.getMillis, dayPeriod)
      val lowestLow = lowest(tsFromTick(period, takeLow), period.last.day.getMillis, dayPeriod)
      TimeSerie(period.last.day, ((period.last.price-lowestLow.value) / (highestHigh.value - lowestLow.value)) * 100)
    }.toList

  def laterOrEqualThan(list: List[TimeSerie], day: Long): List[TimeSerie] = list.filter(_.day.getMillis >= day)

  def flattenQuad(A: List[((TimeSerie, TimeSerie, TimeSerie), TimeSerie)]):
      List[(TimeSerie, TimeSerie, TimeSerie, TimeSerie)] =
    A.map(a => (a._1._1, a._1._2, a._1._3, a._2))

  /**
   * brown = (rsi + mfi + BollOsc + (STOC / 3))/2
   */
  def computeBrown(
      RSI: List[TimeSerie],
      MFI: List[TimeSerie],
      BOSC: List[TimeSerie],
      STOC: List[TimeSerie],
      firstDay: Long): List[TimeSerie] = {
    val rsi = laterOrEqualThan(RSI, firstDay)
    val mfi = laterOrEqualThan(MFI, firstDay)
    val bosc = laterOrEqualThan(BOSC, firstDay)
    val stoc = laterOrEqualThan(STOC, firstDay)
    val trio = (rsi, mfi, bosc).zipped
    val quad = flattenQuad((trio, stoc).zipped.toList)
    quad.map( x => TimeSerie(x._1.day ,(x._1.value + x._2.value + x._3.value + (x._4.value / 3)) / 2))
  }

  def computeGreen(brown: List[TimeSerie], oscp: List[TimeSerie], firstDay: Long): List[TimeSerie] = {
    val m = laterOrEqualThan(brown, firstDay)
    val o = laterOrEqualThan(oscp, firstDay)
    (m, o).zipped.toList.map(x => TimeSerie(x._1.day, (x._1.value + x._2.value)))
  }
}
