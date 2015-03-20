package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object IndexCalculator {

  val CurrentDate = new LocalDate(2013, 12, 31)

  def process(sc: SparkContext, path: String) {
    val ticks = sc.textFile(path).map(Tick.readCsv)
    val close = closingValues(ticks)
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

    val firstDay = new LocalDate(2013, 2, 5)
    val brown = computeBrown(RSI, MFI, bollingerOsc, STOC, firstDay)
    val green = computeGreen(brown, oscp, firstDay)
  }

  /**
   * Compute the values of the ticks when the session closes.
   * @param tickData  the series of tickData
   * @return          the values of the tick at close
   */
  def closingValues(tickData: RDD[Tick]): Seq[ClosingValue] =
    tickData.groupBy(_.date).sortBy { case (date, _) => date }.map {
      case (date, ticks) =>
        val sortedTicks = ticks.toSeq.sortBy(_.time)
        val prices = sortedTicks.map(_.price)
        val closingPrice = prices.last
        val closingVolume = sortedTicks.map(_.volume).sum
        val low = prices.min
        val high = prices.max
        ClosingValue(date, closingPrice, closingVolume, low, high)
    }.collect

  /**
   * Compute the Negative/Positive Volume Index NVI (N/P Volume Index) index:
   * https://www.visualchart.com/esxx/ayuda_F1/Indicators/Volumen/IVN.htm
   * http://www.metastock.com/Customer/Resources/TAAZ/?c=3&p=92
   * @param data      The data over which computing the NVI
   * @return          A list with the index, computed for every change in Tick value.
   */
  def computeVolumeIndex(data: Seq[ClosingValue], matchesVolumeIndex: (Double, Double) => Boolean): Seq[TimeSerie] =
    data.sliding(2).foldLeft(Seq(TimeSerie(data(0).date, 1000.0))) { (index, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      val yesterdaysVI = index.last.value
      val value =
        if (matchesVolumeIndex(secondClosingValue.volume, firstClosingValue.volume)) yesterdaysVI
        else yesterdaysVI * secondClosingValue.price / firstClosingValue.price
      index :+ TimeSerie(secondClosingValue.date, value)
    }

  /**
   * compute Single Exponent Weighted Smoothing to a series of data. Simple method.
   * @param data      the series of index data (date, value)
   * @param alpha     the smoothing parameter (the shorter, the smoother).
   * @return          a list with the smoothed values
   */
  def exponentialAverage(data: Seq[TimeSerie], alpha: Double = 0.1): Seq[TimeSerie] =
    data.foldLeft(Seq[TimeSerie](data(0))) { (accumulator, element) =>
      accumulator :+ TimeSerie(element.date, alpha * element.value + (1 - alpha) * accumulator.last.value)
    }.tail

  def computeOSCP(volIndex: Seq[TimeSerie], sVolIndex: Seq[TimeSerie], viMax: Double, viMin: Double)
      : Seq[TimeSerie] =
    volIndex.zip(sVolIndex).map {
      case (vi, expVI) => TimeSerie(vi.date, ((vi.value - expVI.value) * 100) / (viMax - viMin))
    }

  /* *** Money Flow Index *** */
  def computeTypical(data: Seq[ClosingValue]): Seq[TimeSerie] = data.map { closingValue =>
    TimeSerie(closingValue.date, (closingValue.price + closingValue.high + closingValue.low) / 3)
  }

  def computeRawMoneyFlow(closingValues: Seq[ClosingValue], typical: Seq[TimeSerie]): Seq[TimeSerie] = {
    val volume = closingValues.map(_.volume)
    val rawMoneyFlow = typical.zip(volume).map { case (typData, vol) => TimeSerie(typData.date, typData.value * vol) }
    val initValue = TimeSerie(closingValues(0).date, 1)
    val upDownList = closingValues.sliding(2).foldLeft(Seq[TimeSerie](initValue)) { (upOrDownList, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      upOrDownList :+ TimeSerie(secondClosingValue.date, if (secondClosingValue.price >= firstClosingValue.price) 1 else -1)
    }.tail
    rawMoneyFlow.tail.zip(upDownList).map { case (raw, sign) => TimeSerie(raw.date, raw.value * sign.value) }
  }

  def computeMoneyFlowRatio(rawMoneyFlow: Seq[TimeSerie]): Seq[TimeSerie] =
    rawMoneyFlow.sliding(14).foldLeft(Seq[TimeSerie]()) { (mfrList, windowMF) =>
      val (positives, negatives) = windowMF.foldLeft(z = (0.0, 0.0)) { (z, mf) =>
        if (mf.value >= 0) (z._1 + mf.value, z._2) else (z._1, z._2 + mf.value)
      }
      mfrList :+ TimeSerie(windowMF.last.date, Math.abs(positives / negatives))
    }

  def moneyFlowIndex(closingValues: Seq[ClosingValue]): Seq[TimeSerie] = {
    val typical = computeTypical(closingValues)
    val rawMoneyFlow = computeRawMoneyFlow(closingValues, typical)
    val moneyFlowRatio = computeMoneyFlowRatio(rawMoneyFlow)
    moneyFlowRatio.map(x => TimeSerie(x.date, 100 - (100 / (1 + x.value))))
  }

  def filterIndices(indices: Seq[TimeSerie], currentDate: LocalDate, windowSize: Int): Seq[TimeSerie] =
    indices.filter(_.date <= currentDate).reverseIterator.take(windowSize).toSeq

  def highest(indices: Seq[TimeSerie], currentDate: LocalDate, windowSize: Int): TimeSerie =
    filterIndices(indices, currentDate, windowSize).maxBy(_.value)

  def lowest(indices: Seq[TimeSerie], currentDate: LocalDate, windowSize: Int): TimeSerie =
    filterIndices(indices, currentDate, windowSize).minBy(_.value)

  /**
   * Compute the highest and lowest value in a series of Index Data (date, value) pairs
   * @param indices       The indices to search the max and min over.
   * @param currentDate   The later date for which the max/min are to be searched for.
   * @param windowSize    The number of days to look for the max/min backwards.
   */
  def highestLowest(indices: Seq[TimeSerie], currentDate: LocalDate, windowSize: Int): (TimeSerie, TimeSerie) =
    (highest(indices, currentDate, windowSize), lowest(indices, currentDate, windowSize))

  def mean(xs: Seq[Double]): Double = if (xs.isEmpty) 0.0 else xs.sum / xs.size

  def stddev(xs: Seq[Double]): Double =
    if (xs.isEmpty) 0.0 else math.sqrt((0.0 /: xs) { (a, e) => a + math.pow(e - mean(xs), 2.0) } / xs.size)

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_bands
   */
  def bollingerBands(closignValues: Seq[ClosingValue], windowSize: Int = 25): (Seq[TimeSerie], Seq[TimeSerie]) = {
    closignValues.sliding(windowSize).foldLeft(Seq[(TimeSerie, TimeSerie)]()) { (bollinger, windowTicks) =>
      val priceList = windowTicks.map(tick => TimeSerie(tick.date, tick.price))
      val priceListValues = priceList.map(_.value)
      val sma = mean(priceListValues)
      val stdev = stddev(priceListValues)
      bollinger :+ (TimeSerie(priceList.last.date, sma + (stdev * 2)), TimeSerie(priceList.last.date, sma - (stdev * 2)))
    }.unzip
  }
  
  def bollingerAverage(bUp: Seq[TimeSerie], bDown: Seq[TimeSerie]): Seq[TimeSerie] =
    bUp.zip(bDown).map { case (up, down) => TimeSerie(up.date, (up.value + down.value) / 2) }

  def bollingerRange(bUp: Seq[TimeSerie], bDown: Seq[TimeSerie]): Seq[TimeSerie] =
    bUp.zip(bDown).map { case (up, down) => TimeSerie(up.date, up.value - down.value) }

  /**
   * OB1 = (BollingerUp[25](TotalPrice) + BollingerDown[25](TotalPrice)) / 2
   * OB2 = BollingerUp[25](TotalPrice) - BollingerDown[25](TotalPrice)
   * BollOsc = ((TotalPrice - OB1) / OB2 ) * 100
   */
  def bollingerOscillator(closingValues: Seq[ClosingValue]): Seq[TimeSerie] = {
    val (bUp, bDown) = bollingerBands(closingValues.toSeq)
    val bAvg = bollingerAverage(bUp, bDown)
    val bRng = bollingerRange(bUp, bDown)
    val validTicks = closingValues.drop(closingValues.size - bAvg.size).map(_.price)
    (validTicks, bAvg, bRng).zipped.toSeq.map {
      case (price, avg, rng) => TimeSerie(avg.date, ((price - avg.value) / rng.value) * 100)
    }
  }

  /** RSI **/
  def avgGainLoss(wSize: Int, gainLoss: Seq[TimeSerie], firstAvg: TimeSerie, criteria: TimeSerie => Boolean)
      : Seq[TimeSerie] =
    gainLoss.drop(wSize).foldLeft(Seq[TimeSerie](firstAvg)) { (avgLossList, current) =>
      val currentValue = if (criteria(current)) Math.abs(current.value) else 0
      avgLossList :+ TimeSerie(current.date, Math.abs((avgLossList.last.value * (wSize - 1) + currentValue) / wSize))
    }

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
   * @param closingValues
   * @param windowSize
   * @return
   */
  def relativeStrengthIndex(closingValues: Seq[ClosingValue], windowSize: Int = 14): Seq[TimeSerie] = {
    val gainLoss = closingValues.sliding(2).foldLeft(Seq[TimeSerie]()) { (gainLossList, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      gainLossList :+ TimeSerie(secondClosingValue.date, secondClosingValue.price - firstClosingValue.price)
    }
    val firstRSIDay = gainLoss.take(windowSize + 1).last.date
    val firstAvgGain = TimeSerie(
      firstRSIDay,
      gainLoss.take(windowSize).collect { case ts if ts.value > 0 => ts.value }.sum / windowSize
    )
    val firstAvgLoss = TimeSerie(
      firstRSIDay,
      Math.abs(gainLoss.take(windowSize).collect { case ts if ts.value < 0 => ts.value }.sum / windowSize)
    )
    val avgGain = avgGainLoss(windowSize, gainLoss, firstAvgGain, ts => ts.value >= 0)
    val avgLoss = avgGainLoss(windowSize, gainLoss, firstAvgLoss, ts => ts.value < 0)
    avgGain.zip(avgLoss).map {
      case (gain, loss) =>
        val lossPercentage = if (loss.value == 0) 0 else 100 - (100 / (1 + (gain.value / loss.value)))
        TimeSerie(gain.date, 100 - lossPercentage)
    }
  }

  def stochasticOscillator(closingValues: Seq[ClosingValue], dayPeriod: Int, periodMA: Int): Seq[TimeSerie] = {
    def tsFromTick(values: Seq[ClosingValue], selector: ClosingValue => Double): Seq[TimeSerie] =
      values.map(closingValue => TimeSerie(closingValue.date, selector(closingValue)))
    closingValues.sliding(dayPeriod).map { period =>
      val lastDay = period.last.date
      val highestHigh = highest(tsFromTick(period, closingValue => closingValue.high), lastDay, dayPeriod)
      val lowestLow = lowest(tsFromTick(period, closingValue => closingValue.low), lastDay, dayPeriod)
      TimeSerie(lastDay, ((period.last.price - lowestLow.value) / (highestHigh.value - lowestLow.value)) * 100)
    }.toSeq
  }

  def laterOrEqualThan(list: Seq[TimeSerie], date: LocalDate): Seq[TimeSerie] = list.filter(_.date >= date)

  /**
   * brown = (rsi + mfi + BollOsc + (STOC / 3))/2
   */
  def computeBrown(
      RSI: Seq[TimeSerie],
      MFI: Seq[TimeSerie],
      BOSC: Seq[TimeSerie],
      STOC: Seq[TimeSerie],
      firstDay: LocalDate): Seq[TimeSerie] = {
    val rsi = laterOrEqualThan(RSI, firstDay)
    val mfi = laterOrEqualThan(MFI, firstDay)
    val bosc = laterOrEqualThan(BOSC, firstDay)
    val stoc = laterOrEqualThan(STOC, firstDay)
    val trio = (rsi, mfi, bosc).zipped
    def flattenQuad(quads: Seq[((TimeSerie, TimeSerie, TimeSerie), TimeSerie)])
        : Seq[(TimeSerie, TimeSerie, TimeSerie, TimeSerie)] =
      quads.map(q => (q._1._1, q._1._2, q._1._3, q._2))
    val quad = flattenQuad((trio, stoc).zipped.toSeq)
    quad.map(x => TimeSerie(x._1.date, (x._1.value + x._2.value + x._3.value + (x._4.value / 3)) / 2))
  }

  def computeGreen(brown: Seq[TimeSerie], oscp: Seq[TimeSerie], firstDay: LocalDate): Seq[TimeSerie] = {
    val m = laterOrEqualThan(brown, firstDay)
    val o = laterOrEqualThan(oscp, firstDay)
    (m, o).zipped.toSeq.map(x => TimeSerie(x._1.date, x._1.value + x._2.value))
  }
}
