package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class TickData(day: DateTime, hour: DateTime, price: Double, volume: Double, low: Double, high: Double)
case class TimeSeries(day: DateTime, value: Double)

object IndexCalculator {

  val CurrentDate = new DateTime(2013, 12, 31, 0, 0, 0, 0)

  def process(sc: SparkContext, path: String) {
    val ticks = sc.textFile(path).map { line =>
      val fields = line.split(',')
      TickData(
        day = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(fields(0).substring(0, 10)),
        hour = DateTimeFormat.forPattern("HH:mm:ss").parseDateTime(fields(0).substring(11)),
        price = fields(1).toDouble,
        volume = fields(2).toDouble,
        low = 0,
        high = 0
      )
    }

    val close = closeValues(ticks)

    val PVI = computeVolumeIndex(close, (today, yesterday) => today < yesterday)
    val NVI = computeVolumeIndex(close, (today, yesterday) => today >= yesterday)
    val sPVI = exponentialAverage(PVI)
    val sNVI = exponentialAverage(NVI)
    val (pvimax, pvimin) = highestLowest(sPVI, CurrentDate, sPVI.size)
    val (nvimax, nvimin) = highestLowest(sNVI, CurrentDate, sNVI.size)

    val oscp = computeOSCP(PVI, sPVI, pvimax.value, pvimin.value)
    val blue = computeOSCP(NVI, sNVI, nvimax.value, nvimin.value)

    val MFI = moneyFlowIndex(close)
    val bollingerOsc = bollingerOscillator(close)
    val RSI = relativeStrengthIndex(close)
    val STOC = stochasticOscillator(close.toSeq, 14, 3)

    val firstDay = new DateTime(2013, 2, 5, 0, 0, 0)
    val brown = computeBrown(RSI, MFI, bollingerOsc, STOC, firstDay)
    val green = computeGreen(brown, oscp, firstDay)
  }

  /**
   * Compute the values of the ticks when the session closes.
   * @param tickData  the series of tickData
   * @return          the values of the tick at close
   */
  def closeValues(tickData: RDD[TickData]): Seq[TickData] = {
    val midnight = new DateTime(1970, 1, 1, 23, 59, 59)
    tickData.groupBy(_.day).sortBy { case (day, _) => day }.map {
      case (day, ticks) =>
        val prices = ticks.map(_.price)
        val closePrice = prices.last
        val closeVolume = ticks.map(_.volume).sum
        val low = prices.min
        val high = prices.max
        TickData(day, midnight, closePrice, closeVolume, low, high)
    }.collect
  }

  /**
   * Compute the IVN/P (N/P Volume Index) index:
   * https://www.visualchart.com/esxx/ayuda_F1/Indicators/Volumen/IVN.htm
   * http://www.metastock.com/Customer/Resources/TAAZ/?c=3&p=92
   * @param data      The data over which computing the INV index
   * @return          A list with the index, computed for every change in Tick value.
   */
  def computeVolumeIndex(data: Seq[TickData], matchesVolumeIndex: (Double, Double) => Boolean): Seq[TimeSeries] = {
    val initValue = Seq(TimeSeries(data(0).day, 1000.0))
    data.sliding(2).foldLeft(initValue) { (index, pair) =>
      val yesterdaysVI = index.last.value
      val value =
        if (matchesVolumeIndex(pair(1).volume, pair(0).volume)) yesterdaysVI
        else {
          def percentagePriceChange(pair: Seq[TickData]): Double =
            pair(1).price/pair(0).price
          yesterdaysVI * percentagePriceChange(pair)
        }
      index :+ TimeSeries(pair(1).day, value)
    }
  }

  /**
   * compute Single Exponent Weighted Smoothing to a series of data. Simple method.
   * @param data      the series of index data (date, value)
   * @param alpha     the smoothing parameter (the shorter, the smoother).
   * @return          a list with the smoothed values
   */
  def exponentialAverage(data: Seq[TimeSeries], alpha: Double = 0.1): Seq[TimeSeries] =
    data.foldLeft(Seq[TimeSeries](data(0))) { (accumulator, element) =>
      accumulator :+ TimeSeries(element.day, alpha * element.value + (1 - alpha) * accumulator.last.value)
    }.tail

  def printTimeSeriesValues(series: Seq[TimeSeries]) {
    series.foreach(ts => println(ts.value))
  }

  def computeOSCP(volIndex: Seq[TimeSeries], sVolIndex: Seq[TimeSeries], viMax: Double, viMin: Double)
      : Seq[TimeSeries] =
    volIndex.zip(sVolIndex).map {
      case (vi, expVI) => TimeSeries(vi.day, ((vi.value - expVI.value) * 100) / (viMax - viMin))
    }

  /* *** Money Flow Index *** */
  def computeTypical(data: Seq[TickData]): Seq[TimeSeries] =
    data.map(tick => TimeSeries(tick.day, (tick.price * tick.high * tick.low) / 3))

  def computeRawMoneyFlow(ticks: Seq[TickData], typical: Seq[TimeSeries]): Seq[TimeSeries] = {
    val volume = ticks.map(_.volume)
    val rawMoneyFlow = typical.zip(volume).map { case (typData, vol) => TimeSeries(typData.day, typData.value * vol) }
    val initValue = TimeSeries(ticks(0).day, 1)
    val upDownList = ticks.sliding(2).foldLeft(Seq[TimeSeries](initValue)) { (upOrDownList, pair) =>
      upOrDownList :+ TimeSeries(pair(1).day, if (pair(1).price >= pair(0).price) 1 else -1)
    }.tail
    rawMoneyFlow.tail.zip(upDownList).map { case (raw, sign) => TimeSeries(raw.day, raw.value * sign.value) }
  }

  def computeMoneyFlowRatio(rawMoneyFlow: Seq[TimeSeries]): Seq[TimeSeries] =
    rawMoneyFlow.sliding(14).foldLeft(Seq[TimeSeries]()) { (mfrList, windowMF) =>
      val initValue = 0.0
      val (positives, negatives) = windowMF.foldLeft(z = (initValue, initValue)) { (z, mf) =>
        if (mf.value >= 0) (z._1 + mf.value, z._2) else (z._1, z._2 + mf.value)
      }
      mfrList :+ TimeSeries(windowMF.last.day, Math.abs(positives / negatives))
    }

  def moneyFlowIndex(close: Seq[TickData]): Seq[TimeSeries] = {
    val typical = computeTypical(close)
    val rawMoneyFlow = computeRawMoneyFlow(close, typical)
    val moneyFlowRatio = computeMoneyFlowRatio(rawMoneyFlow)
    moneyFlowRatio.map(x => TimeSeries(x.day, 100 - (100 / (1 + x.value))))
  }

  /**
   * Compute the highest and lowest value in a series of Index Data (date, value) pairs
   * @param indices       The indices to search the max and min over.
   * @param currentDate   The later date for which the max/min are to be searched for.
   * @param windowSize    The number of days to look for the max/min backwards.
   */
  def highestLowest(indices: Seq[TimeSeries], currentDate: DateTime, windowSize: Int): (TimeSeries, TimeSeries) =
    (highest(indices, currentDate, windowSize), lowest(indices, currentDate, windowSize))

  def highest(indices: Seq[TimeSeries], currentDate: DateTime, windowSize: Int): TimeSeries =
    filterIndices(indices, currentDate, windowSize).maxBy(_.value)

  def lowest(indices: Seq[TimeSeries], currentDate: DateTime, windowSize: Int): TimeSeries =
    filterIndices(indices, currentDate, windowSize).minBy(_.value)

  def filterIndices(indices: Seq[TimeSeries], currentDate: DateTime, windowSize: Int): Seq[TimeSeries] =
    indices.filter(_.day <= currentDate).reverseIterator.take(windowSize).toSeq

  def mean(xs: Seq[Double]): Double = if (xs.isEmpty) 0.0 else xs.sum / xs.size

  def stddev(xs: Seq[Double]): Double =
    if (xs.isEmpty) 0.0 else math.sqrt((0.0 /: xs) { (a, e) => a + math.pow(e - mean(xs), 2.0) } / xs.size)

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_bands
   */
  def bollingerBands(data: Seq[TickData], windowSize: Int = 25): (Seq[TimeSeries], Seq[TimeSeries]) = {
    data.sliding(windowSize).foldLeft(Seq[(TimeSeries, TimeSeries)]()) { (bollinger, windowTicks) =>
      val priceList = windowTicks.map(tick => TimeSeries(tick.day, tick.price))
      val priceListValues = priceList.map(_.value)
      val sma = mean(priceListValues)
      val stdev = stddev(priceListValues)
      bollinger :+ (TimeSeries(priceList.last.day, sma + (stdev * 2)), TimeSeries(priceList.last.day, sma - (stdev * 2)))
    }.unzip
  }
  
  def bollingerAverage(bUp: Seq[TimeSeries], bDown: Seq[TimeSeries]): Seq[TimeSeries] =
    bUp.zip(bDown).map { case (up, down) => TimeSeries(up.day, (up.value + down.value) / 2) }

  def bollingerRange(bUp: Seq[TimeSeries], bDown: Seq[TimeSeries]): Seq[TimeSeries] =
    bUp.zip(bDown).map { case (up, down) => TimeSeries(up.day, up.value - down.value) }

  /**
   * OB1 = (BollingerUp[25](TotalPrice) + BollingerDown[25](TotalPrice)) / 2
   * OB2 = BollingerUp[25](TotalPrice) - BollingerDown[25](TotalPrice)
   * BollOsc = ((TotalPrice - OB1) / OB2 ) * 100
   */
  def bollingerOscillator(ticks: Seq[TickData]): Seq[TimeSeries] = {
    val (bUp, bDown) = bollingerBands(ticks.toSeq)
    val bAvg = bollingerAverage(bUp, bDown)
    val bRng = bollingerRange(bUp, bDown)
    val validTicks = ticks.drop(ticks.size - bAvg.size).map(_.price)
    (validTicks, bAvg, bRng).zipped.toSeq.map {
      case (price, avg, rng) => TimeSeries(avg.day, ((price - avg.value) / rng.value) * 100)
    }
  }

  /** RSI **/
  def avgGainLoss(wSize: Int, gainLoss: Seq[TimeSeries], firstAvg: TimeSeries, criteria: TimeSeries => Boolean)
      : Seq[TimeSeries] =
    gainLoss.drop(wSize).foldLeft(Seq[TimeSeries](firstAvg)) { (avgLossList, current) =>
      val currentValue = if (criteria(current)) Math.abs(current.value) else 0
      avgLossList :+ TimeSeries(current.day, Math.abs((avgLossList.last.value * (wSize - 1) + currentValue) / wSize))
    }

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
   * @param close
   * @param windowSize
   * @return
   */
  def relativeStrengthIndex(close: Seq[TickData], windowSize: Int = 14): Seq[TimeSeries] = {
    val gainLoss = close.sliding(2).foldLeft(Seq[TimeSeries]()) { (gainLossList, pair) =>
      gainLossList :+ TimeSeries(pair(1).day, pair(1).price - pair(0).price)
    }
    val firstRSIDay = gainLoss.take(windowSize + 1).last.day
    val firstAvgGain = TimeSeries(
      firstRSIDay,
      gainLoss.take(windowSize).collect { case ts if ts.value > 0 => ts.value }.sum / windowSize
    )
    val firstAvgLoss = TimeSeries(
      firstRSIDay,
      Math.abs(gainLoss.take(windowSize).collect { case ts if ts.value < 0 => ts.value }.sum / windowSize)
    )
    val avgGain = avgGainLoss(windowSize, gainLoss, firstAvgGain, ts => ts.value >= 0)
    val avgLoss = avgGainLoss(windowSize, gainLoss, firstAvgLoss, ts => ts.value < 0)
    avgGain.zip(avgLoss).map {
      case (gain, loss) =>
        val lossPercentage = if (loss.value == 0) 0 else 100 - (100 / (1 + (gain.value / loss.value)))
        TimeSeries(gain.day, 100 - lossPercentage)
    }
  }

  def stochasticOscillator(close: Seq[TickData], dayPeriod: Int, periodMA: Int): Seq[TimeSeries] = {
    def tsFromTick(ticks: Seq[TickData], selector: TickData => Double): Seq[TimeSeries] =
      ticks.map(tick => TimeSeries(tick.day, selector(tick)))
    close.sliding(dayPeriod).map { period =>
      val lastDay = period.last.day
      val highestHigh = highest(tsFromTick(period, tick => tick.high), lastDay, dayPeriod)
      val lowestLow = lowest(tsFromTick(period, tick => tick.low), lastDay, dayPeriod)
      TimeSeries(lastDay, ((period.last.price - lowestLow.value) / (highestHigh.value - lowestLow.value)) * 100)
    }.toSeq
  }

  def laterOrEqualThan(list: Seq[TimeSeries], day: DateTime): Seq[TimeSeries] = list.filter(_.day >= day)

  /**
   * brown = (rsi + mfi + BollOsc + (STOC / 3))/2
   */
  def computeBrown(
      RSI: Seq[TimeSeries],
      MFI: Seq[TimeSeries],
      BOSC: Seq[TimeSeries],
      STOC: Seq[TimeSeries],
      firstDay: DateTime): Seq[TimeSeries] = {
    val rsi = laterOrEqualThan(RSI, firstDay)
    val mfi = laterOrEqualThan(MFI, firstDay)
    val bosc = laterOrEqualThan(BOSC, firstDay)
    val stoc = laterOrEqualThan(STOC, firstDay)
    val trio = (rsi, mfi, bosc).zipped
    def flattenQuad(quads: Seq[((TimeSeries, TimeSeries, TimeSeries), TimeSeries)])
        : Seq[(TimeSeries, TimeSeries, TimeSeries, TimeSeries)] =
      quads.map(q => (q._1._1, q._1._2, q._1._3, q._2))
    val quad = flattenQuad((trio, stoc).zipped.toSeq)
    quad.map(x => TimeSeries(x._1.day, (x._1.value + x._2.value + x._3.value + (x._4.value / 3)) / 2))
  }

  def computeGreen(brown: Seq[TimeSeries], oscp: Seq[TimeSeries], firstDay: DateTime): Seq[TimeSeries] = {
    val m = laterOrEqualThan(brown, firstDay)
    val o = laterOrEqualThan(oscp, firstDay)
    (m, o).zipped.toSeq.map(x => TimeSeries(x._1.day, x._1.value + x._2.value))
  }
}
