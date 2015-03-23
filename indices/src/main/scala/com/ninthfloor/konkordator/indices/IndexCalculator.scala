package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object IndexCalculator {

  val CurrentDate = new LocalDate(2013, 12, 31)

  def process(sc: SparkContext, path: String) {
    val ticks = sc.textFile(path).map(Tick.readCsv)
    val close = closingValues(ticks)
    val pvi = computeVolumeIndex(close, _ < _)
    val nvi = computeVolumeIndex(close, _ >= _)
    val sPvi = exponentialAverage(pvi)
    val sNvi = exponentialAverage(nvi)
    val (pvimax, pvimin) = highestLowest(sPvi, CurrentDate, sPvi.size)
    val (nvimax, nvimin) = highestLowest(sNvi, CurrentDate, sNvi.size)

    val oscp = computeOSCP(pvi, sPvi, pvimax.value, pvimin.value)
    val blue = computeOSCP(nvi, sNvi, nvimax.value, nvimin.value)

    val mfi = moneyFlowIndex(close)
    val bosc = bollingerOscillator(close)
    val rsi = relativeStrengthIndex(close)
    val stoc = stochasticOscillator(close, 14, 3)

    val firstDay = new LocalDate(2013, 2, 5)
    val brown = computeBrown(rsi, mfi, bosc, stoc, firstDay)
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
  def computeVolumeIndex(data: Seq[ClosingValue], matchesVolumeIndex: (Double, Double) => Boolean): Seq[IndexValue] =
    data.sliding(2).foldLeft(Seq(IndexValue(data(0).date, 1000.0))) { (index, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      val yesterdaysVI = index.last.value
      val value =
        if (matchesVolumeIndex(secondClosingValue.volume, firstClosingValue.volume)) yesterdaysVI
        else yesterdaysVI * secondClosingValue.price / firstClosingValue.price
      index :+ IndexValue(secondClosingValue.date, value)
    }

  /**
   * compute Single Exponent Weighted Smoothing to a series of data. Simple method.
   * @param data      the series of index data (date, value)
   * @param alpha     the smoothing parameter (the shorter, the smoother).
   * @return          a list with the smoothed values
   */
  def exponentialAverage(data: Seq[IndexValue], alpha: Double = 0.1): Seq[IndexValue] =
    data.foldLeft(Seq[IndexValue](data(0))) { (accumulator, element) =>
      accumulator :+ IndexValue(element.date, alpha * element.value + (1 - alpha) * accumulator.last.value)
    }.tail

  def computeOSCP(volIndex: Seq[IndexValue], sVolIndex: Seq[IndexValue], viMax: Double, viMin: Double)
      : Seq[IndexValue] =
    volIndex.zip(sVolIndex).map {
      case (vi, expVI) => IndexValue(vi.date, ((vi.value - expVI.value) * 100) / (viMax - viMin))
    }

  /* *** Money Flow Index *** */
  def computeTypical(data: Seq[ClosingValue]): Seq[IndexValue] = data.map { closingValue =>
    IndexValue(closingValue.date, (closingValue.price + closingValue.high + closingValue.low) / 3)
  }

  def computeRawMoneyFlow(closingValues: Seq[ClosingValue], typical: Seq[IndexValue]): Seq[IndexValue] = {
    val volume = closingValues.map(_.volume)
    val rawMoneyFlow = typical.zip(volume).map { case (typData, vol) => IndexValue(typData.date, typData.value * vol) }
    val initValue = IndexValue(closingValues(0).date, 1)
    val upDownList = closingValues.sliding(2).foldLeft(Seq[IndexValue](initValue)) { (upOrDownList, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      upOrDownList :+ IndexValue(secondClosingValue.date, if (secondClosingValue.price >= firstClosingValue.price) 1 else -1)
    }.tail
    rawMoneyFlow.tail.zip(upDownList).map { case (raw, sign) => IndexValue(raw.date, raw.value * sign.value) }
  }

  def computeMoneyFlowRatio(rawMoneyFlow: Seq[IndexValue]): Seq[IndexValue] =
    rawMoneyFlow.sliding(14).foldLeft(Seq[IndexValue]()) { (mfrList, windowMF) =>
      val (positives, negatives) = windowMF.foldLeft(z = (0.0, 0.0)) { (z, mf) =>
        if (mf.value >= 0) (z._1 + mf.value, z._2) else (z._1, z._2 + mf.value)
      }
      mfrList :+ IndexValue(windowMF.last.date, Math.abs(positives / negatives))
    }

  def moneyFlowIndex(closingValues: Seq[ClosingValue]): Seq[IndexValue] = {
    val typical = computeTypical(closingValues)
    val rawMoneyFlow = computeRawMoneyFlow(closingValues, typical)
    val moneyFlowRatio = computeMoneyFlowRatio(rawMoneyFlow)
    moneyFlowRatio.map(x => IndexValue(x.date, 100 - (100 / (1 + x.value))))
  }

  def filterIndices(indices: Seq[IndexValue], currentDate: LocalDate, windowSize: Int): Seq[IndexValue] =
    indices.filter(_.date <= currentDate).reverseIterator.take(windowSize).toSeq

  def highest(indices: Seq[IndexValue], currentDate: LocalDate, windowSize: Int): IndexValue =
    filterIndices(indices, currentDate, windowSize).maxBy(_.value)

  def lowest(indices: Seq[IndexValue], currentDate: LocalDate, windowSize: Int): IndexValue =
    filterIndices(indices, currentDate, windowSize).minBy(_.value)

  /**
   * Compute the highest and lowest value in a series of Index Data (date, value) pairs
   * @param indices       The indices to search the max and min over.
   * @param currentDate   The later date for which the max/min are to be searched for.
   * @param windowSize    The number of days to look for the max/min backwards.
   */
  def highestLowest(indices: Seq[IndexValue], currentDate: LocalDate, windowSize: Int): (IndexValue, IndexValue) =
    (highest(indices, currentDate, windowSize), lowest(indices, currentDate, windowSize))

  def mean(xs: Seq[Double]): Double = if (xs.isEmpty) 0.0 else xs.sum / xs.size

  def stddev(xs: Seq[Double]): Double =
    if (xs.isEmpty) 0.0 else math.sqrt((0.0 /: xs) { (a, e) => a + math.pow(e - mean(xs), 2.0) } / xs.size)

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_bands
   */
  def bollingerBands(closignValues: Seq[ClosingValue], windowSize: Int = 25): (Seq[IndexValue], Seq[IndexValue]) = {
    closignValues.sliding(windowSize).foldLeft(Seq[(IndexValue, IndexValue)]()) { (bollinger, windowTicks) =>
      val priceList = windowTicks.map(tick => IndexValue(tick.date, tick.price))
      val priceListValues = priceList.map(_.value)
      val sma = mean(priceListValues)
      val stdev = stddev(priceListValues)
      bollinger :+ (IndexValue(priceList.last.date, sma + (stdev * 2)), IndexValue(priceList.last.date, sma - (stdev * 2)))
    }.unzip
  }
  
  def bollingerAverage(bUp: Seq[IndexValue], bDown: Seq[IndexValue]): Seq[IndexValue] =
    bUp.zip(bDown).map { case (up, down) => IndexValue(up.date, (up.value + down.value) / 2) }

  def bollingerRange(bUp: Seq[IndexValue], bDown: Seq[IndexValue]): Seq[IndexValue] =
    bUp.zip(bDown).map { case (up, down) => IndexValue(up.date, up.value - down.value) }

  /**
   * OB1 = (BollingerUp[25](TotalPrice) + BollingerDown[25](TotalPrice)) / 2
   * OB2 = BollingerUp[25](TotalPrice) - BollingerDown[25](TotalPrice)
   * BollOsc = ((TotalPrice - OB1) / OB2 ) * 100
   */
  def bollingerOscillator(closingValues: Seq[ClosingValue]): Seq[IndexValue] = {
    val (bUp, bDown) = bollingerBands(closingValues.toSeq)
    val bAvg = bollingerAverage(bUp, bDown)
    val bRng = bollingerRange(bUp, bDown)
    val validTicks = closingValues.drop(closingValues.size - bAvg.size).map(_.price)
    (validTicks, bAvg, bRng).zipped.toSeq.map {
      case (price, avg, rng) => IndexValue(avg.date, ((price - avg.value) / rng.value) * 100)
    }
  }

  /** RSI **/
  def avgGainLoss(wSize: Int, gainLoss: Seq[IndexValue], firstAvg: IndexValue, criteria: IndexValue => Boolean)
      : Seq[IndexValue] =
    gainLoss.drop(wSize).foldLeft(Seq[IndexValue](firstAvg)) { (avgLossList, current) =>
      val currentValue = if (criteria(current)) Math.abs(current.value) else 0
      avgLossList :+ IndexValue(current.date, Math.abs((avgLossList.last.value * (wSize - 1) + currentValue) / wSize))
    }

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
   * @param closingValues
   * @param windowSize
   * @return
   */
  def relativeStrengthIndex(closingValues: Seq[ClosingValue], windowSize: Int = 14): Seq[IndexValue] = {
    val gainLoss = closingValues.sliding(2).foldLeft(Seq[IndexValue]()) { (gainLossList, pair) =>
      val Seq(firstClosingValue, secondClosingValue) = pair
      gainLossList :+ IndexValue(secondClosingValue.date, secondClosingValue.price - firstClosingValue.price)
    }
    val firstRSIDay = gainLoss.take(windowSize + 1).last.date
    val firstAvgGain = IndexValue(
      firstRSIDay,
      gainLoss.take(windowSize).collect { case ts if ts.value > 0 => ts.value }.sum / windowSize
    )
    val firstAvgLoss = IndexValue(
      firstRSIDay,
      Math.abs(gainLoss.take(windowSize).collect { case ts if ts.value < 0 => ts.value }.sum / windowSize)
    )
    val avgGain = avgGainLoss(windowSize, gainLoss, firstAvgGain, _.value >= 0)
    val avgLoss = avgGainLoss(windowSize, gainLoss, firstAvgLoss, _.value < 0)
    avgGain.zip(avgLoss).map {
      case (gain, loss) =>
        val lossPercentage = if (loss.value == 0) 0 else 100 - (100 / (1 + (gain.value / loss.value)))
        IndexValue(gain.date, 100 - lossPercentage)
    }
  }

  def stochasticOscillator(closingValues: Seq[ClosingValue], dayPeriod: Int, periodMA: Int): Seq[IndexValue] = {
    def tsFromTick(values: Seq[ClosingValue], selector: ClosingValue => Double): Seq[IndexValue] =
      values.map(closingValue => IndexValue(closingValue.date, selector(closingValue)))
    closingValues.sliding(dayPeriod).map { period =>
      val lastDay = period.last.date
      val highestHigh = highest(tsFromTick(period, _.high), lastDay, dayPeriod)
      val lowestLow = lowest(tsFromTick(period, _.low), lastDay, dayPeriod)
      IndexValue(lastDay, ((period.last.price - lowestLow.value) / (highestHigh.value - lowestLow.value)) * 100)
    }.toSeq
  }

  def laterOrEqualThan(list: Seq[IndexValue], date: LocalDate): Seq[IndexValue] = list.filter(_.date >= date)

  /**
   * brown = (rsi + mfi + BollOsc + (STOC / 3))/2
   */
  def computeBrown(
      RSI: Seq[IndexValue],
      MFI: Seq[IndexValue],
      BOSC: Seq[IndexValue],
      STOC: Seq[IndexValue],
      firstDay: LocalDate): Seq[IndexValue] = {
    val filteredRsi = laterOrEqualThan(RSI, firstDay)
    val filteredMfi = laterOrEqualThan(MFI, firstDay)
    val filteredBosc = laterOrEqualThan(BOSC, firstDay)
    val filteredStoc = laterOrEqualThan(STOC, firstDay)
    ((filteredRsi, filteredMfi, filteredBosc).zipped, filteredStoc).zipped.map {
      case ((rsiItem, mfiItem, boscItem), stocItem) =>
        IndexValue(rsiItem.date, (rsiItem.value + mfiItem.value + boscItem.value + (stocItem.value / 3)) / 2)
    }.toSeq
  }

  def computeGreen(brown: Seq[IndexValue], oscp: Seq[IndexValue], firstDay: LocalDate): Seq[IndexValue] = {
    val filteredBrown = laterOrEqualThan(brown, firstDay)
    val filteredOscp = laterOrEqualThan(oscp, firstDay)
    (filteredBrown, filteredOscp).zipped.toSeq.map {
      case (brownItem, oscpItem) => IndexValue(brownItem.date, brownItem.value + oscpItem.value)
    }
  }
}
