package com.renero.trader.indices.process

import org.apache.commons.math.stat.descriptive.{SynchronizedDescriptiveStatistics, SummaryStatistics}
import org.apache.spark.SparkContext
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class TickData(day:DateTime,hour:DateTime,price:Double,volume:Double,low:Double,high:Double)
case class TimeSerie(day:DateTime,value:Double)

object process {

  val currentDate = new DateTime(2013, 12, 31, 0, 0, 0, 0).getMillis

  def process(sc:SparkContext) {
    val file="/Users/renero/Documents/SideProjects/X/REPSOL.csv"
    val tickData = sc.textFile(file).map(_.split(',')).map {
      case (array) => new TickData(
        DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(array(0).substring(0,10)),
        DateTimeFormat.forPattern("HH:mm:ss").parseDateTime(array(0).substring(11)),
        array(1).toDouble, array(2).toDouble, 0, 0)
    }

    val close = closeValues(tickData)

    val PVI = computeVolumeIndex(close, positiveVolumeIndex)
    val NVI = computeVolumeIndex(close, negativeVolumeIndex)
    val sPVI = exponentialAverage(PVI)
    val sNVI = exponentialAverage(NVI)
    val (pvimax,pvimin) = highestLowest(sPVI, currentDate, sPVI.size)
    val (nvimax,nvimin) = highestLowest(sNVI, currentDate, sNVI.size)

    val oscp = computeOSCP(PVI, sPVI, pvimax.value, pvimin.value)
    val azul = computeOSCP(NVI, sNVI, nvimax.value, nvimin.value)

    val MFI = moneyFlowIndex(close)
    val bollingerOsc = bollingerOscillator(close)
    val RSI = relativeStrengthIndex(close)
    val STOC = stochasticOscillator(close.toList, 14, 3)

    val firstDay = new DateTime(2013, 2, 5, 0, 0, 0, 0).getMillis
    val marron = computeMarron(RSI, MFI, bollingerOsc, STOC, firstDay)
    val verde = computeVerde(marron, oscp, firstDay)
  }

  /**
   * Compute the values of the ticks when the session closes.
   * @param tickData  the series of tickData
   * @return          the values of the tick at close
   */
  def closeValues(tickData:RDD[TickData]):Array[TickData] = {
    val midnight = new DateTime(1970, 1, 1, 23, 59, 59, 0)
    val ticksGroupedByDate = tickData.groupBy(_.day).sortBy(_._1)
    ticksGroupedByDate.collect.map { pair =>
      val day = pair._1
      val closePrice = pair._2.last.price
      val closeVolume = pair._2.foldLeft(0.0f:Double)(_ + _.volume)
      val low  = pair._2.foldLeft(pair._2.head.price)((min, tick) => if (tick.price < min) tick.price else min)
      val high = pair._2.foldLeft(pair._2.head.price)((max, tick) => if (tick.price > max) tick.price else max)
      TickData(day, midnight, closePrice, closeVolume, low, high)
    }
  }

  def percentagePriceChange(pair: Array[TickData]) : Double = ((pair(1).price - pair(0).price) / pair(0).price)*100
  def positiveVolumeIndex(today:Double, yesterday:Double):Boolean = if(today >= yesterday) false else true
  def negativeVolumeIndex(today:Double, yesterday:Double):Boolean = if(today < yesterday) false else true

  /**
   * Compute the IVN/P (N/P Volume Index) index:
   * https://www.visualchart.com/esxx/ayuda_F1/Indicators/Volumen/IVN.htm
   * http://www.metastock.com/Customer/Resources/TAAZ/?c=3&p=92
   * @param data      The data over which computing the INV index
   * @return          A list with the index, computed for evey change in Tick value.
   */
  def computeVolumeIndex(data:Array[TickData], matchesVolumeIndexLambda:(Double,Double) => Boolean):
      List[TimeSerie] = {
    val initValue = TimeSerie(data(0).day, 1000.0)
    data.sliding(2).foldLeft(List[TimeSerie](initValue)) {
      (indice,pair) =>
        if (matchesVolumeIndexLambda(pair(1).volume, pair(0).volume))
          indice :+ TimeSerie(pair(1).day, indice.reverse.head.value)
        else {
          val yesterdaysVI = indice.reverse.head.value
          indice :+ TimeSerie(pair(1).day, yesterdaysVI + percentagePriceChange(pair))
        }
    }
  }

  /**
   * compute Single Exponent Weighted Smoothing to a series of data. Simple method.
   * @param data      the series of index data (date, value)
   * @param alpha     the smoothing parameter (the shorter, the smoother).
   * @return          a list with the smoothed values
   */
  def exponentialAverage(data:List[TimeSerie], alpha:Double=0.1):List[TimeSerie] = {
    data.foldLeft(List[TimeSerie](data(0))) {
      (S, Y) =>
        S :+ TimeSerie(Y.day, alpha*Y.value + (1-alpha)*S.reverse.head.value)
    }.tail
  }

  def prints(serie: List[TimeSerie]) {
    serie.map(_.value).foreach(println)
  }

  def computeOSCP(volIndex:List[TimeSerie], sVolIndex:List[TimeSerie], viMax:Double, viMin:Double):
      List[TimeSerie] = {
    volIndex.zip(sVolIndex).map {
      case (vi, expVI) => TimeSerie(vi.day, ((vi.value-expVI.value)*100)/(viMax-viMin))
    }
  }

  /* *** Money Flow Index *** */

  def computeTypical(data:Array[TickData]):Array[TimeSerie] = {
    data.map(tick => TimeSerie(tick.day, (tick.price * tick.high * tick.low) / 3))
  }

  def computeRawMoneyFlow(ticks:Array[TickData], typical:Array[TimeSerie]):Array[TimeSerie] = {
    val volume = ticks.map( t => t.volume )
    val rawMoneyFlow = typical.zip(volume).map {
      case(typData, vol) => TimeSerie(typData.day, typData.value * vol)
    }
    val initValue = TimeSerie(ticks(0).day, 1)
    val upDownList = ticks.sliding(2).foldLeft(List[TimeSerie](initValue)){
      (upOrDownList, pair) =>
        if (pair(1).price >= pair(0).price) upOrDownList :+ TimeSerie(pair(1).day, 1)
        else upOrDownList :+ TimeSerie(pair(1).day, -1)
    }.tail
    rawMoneyFlow.tail.zip(upDownList).map {
      case (raw, sign) => TimeSerie(raw.day, raw.value * sign.value)
    }
  }

  def computeMoneyFlowRatio(rawMoneyFlow:Array[TimeSerie]):List[TimeSerie] = {
    rawMoneyFlow.sliding(14).foldLeft(List[TimeSerie]()) {
      (mfrList, windowMF) =>
        val initValue:Double = 0.0
        val (positives,negatives) = windowMF.foldLeft(z = (initValue, initValue)) {
          (z, mf) =>
            if(mf.value >= 0) (z._1 + mf.value, z._2)
            else (z._1, z._2 + mf.value)
        }
        mfrList :+ TimeSerie(windowMF.last.day, Math.abs(positives/negatives))
    }
  }

  def moneyFlowIndex(close:Array[TickData]):List[TimeSerie] = {
    val typical = computeTypical(close)
    val rawMoneyFlow = computeRawMoneyFlow(close, typical)
    val moneyFlowRatio = computeMoneyFlowRatio(rawMoneyFlow)
    moneyFlowRatio.map( x => TimeSerie(x.day, 100 - (100/(1+x.value))) )
  }

  /**
   * Compute the highest and lowest value in a series of Index Data (date, value) pairs
   * @param indices       The indices to search the max and min over.
   * @param currentDate   The later date for which the max/min are to be searched for.
   * @param windowSize    The number of days to look for the max/min backwards.
   */
  def highestLowest(indices:List[TimeSerie], currentDate:Long, windowSize:Int):(TimeSerie,TimeSerie) = {
    val windowValues = indices.filter( i => i.day.getMillis <= currentDate ).reverseIterator.take(windowSize).toList
    val max = windowValues.foldLeft(indices(0)) ((max, x) => if (x.value > max.value) x else max)
    val min = windowValues.foldLeft(indices(0)) ((min, x) => if (x.value < min.value) x else min)
    (max, min)
  }

  def highest(indices:List[TimeSerie], currentDate:Long, windowSize:Int):TimeSerie = {
    val windowValues = indices.filter( i => i.day.getMillis <= currentDate ).reverseIterator.take(windowSize).toList
    windowValues.foldLeft(indices(0)) ((max, x) => if (x.value > max.value) x else max)
  }

  def lowest(indices:List[TimeSerie], currentDate:Long, windowSize:Int):TimeSerie = {
    val windowValues = indices.filter( i => i.day.getMillis <= currentDate ).reverseIterator.take(windowSize).toList
    windowValues.foldLeft(indices(0)) ((min, x) => if (x.value < min.value) x else min)
  }

  def mean(xs: List[Double]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }
  def stddev(xs: List[Double], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a,e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:bollinger_bands
   */
  def bollingerBands(data:List[TickData], windowSize:Int=25):(List[TimeSerie],List[TimeSerie]) = {
    data.sliding(windowSize).foldLeft(List[(TimeSerie, TimeSerie)]()) {
      (bollinger, windowTicks) =>
        val priceList = windowTicks.map(tick => TimeSerie(tick.day, tick.price))
        val sma = mean(priceList.map(_.value))
        val stdev = stddev(priceList.map(_.value), mean(priceList.map(_.value)))
        bollinger :+(TimeSerie(priceList.last.day, sma + (stdev*2)), TimeSerie(priceList.last.day, sma - (stdev*2)))
    }.unzip
  }
  
  def bollingerAverage(bUp:List[TimeSerie],bDown:List[TimeSerie]):List[TimeSerie] = bUp.zip(bDown).map {
    case (up, down) => TimeSerie(up.day, (up.value + down.value) / 2)
  }

  def bollingerRange(bUp:List[TimeSerie],bDown:List[TimeSerie]):List[TimeSerie] = bUp.zip(bDown).map {
    case (up, down) => TimeSerie(up.day, up.value - down.value)
  }

  /**
   * OB1 = (BollingerUp[25](TotalPrice) + BollingerDown[25](TotalPrice)) / 2
   * OB2 = BollingerUp[25](TotalPrice) - BollingerDown[25](TotalPrice)
   * BollOsc = ((TotalPrice - OB1) / OB2 ) * 100
   */
  def bollingerOscillator(ticks:Array[TickData]):List[TimeSerie] = {
    val (bUp, bDown) = bollingerBands(ticks.toList)
    val bAvg = bollingerAverage(bUp, bDown)
    val bRng = bollingerRange(bUp, bDown)
    val validTicks = ticks.drop(ticks.size-bAvg.size).map( _.price )
    (validTicks, bAvg, bRng).zipped.toList.map {
      case (price, avg, rng) => TimeSerie( avg.day, ((price - avg.value) / rng.value)*100 )
    }
  }

  /** RSI **/
  def positive(ts:TimeSerie):Boolean = if (ts.value >= 0) true else false
  def negative(ts:TimeSerie):Boolean = !positive(ts)
  def avgGainLoss(wSize:Int, gainLoss:List[TimeSerie], firstAvg:TimeSerie, criteria:TimeSerie => Boolean):
      List[TimeSerie] = {
    gainLoss.drop(wSize).foldLeft(List[TimeSerie](firstAvg)) { (avgLossList, current) =>
      if (criteria(current)) {
        avgLossList :+ TimeSerie(current.day,
          Math.abs(((avgLossList.reverse.head.value * (wSize - 1)) + Math.abs(current.value)) / wSize))
      }
      else
        avgLossList :+ TimeSerie(current.day, Math.abs(avgLossList.reverse.head.value * (wSize - 1) / wSize))
    }
  }
  /**
   * http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi
   * @param close
   * @param windowSize
   * @return
   */
  def relativeStrengthIndex(close:Array[TickData], windowSize:Int=14):List[TimeSerie] = {
    val gainLoss = close.sliding(2).foldLeft(List[TimeSerie]()) {
      (gainLossList, pair) => gainLossList :+ TimeSerie(pair(1).day, pair(1).price - pair(0).price)
    }
    val firstRSIDay = gainLoss.take(windowSize+1).reverse.head.day
    val firstAvgGain = TimeSerie(firstRSIDay,
      gainLoss.take(windowSize).filter(_.value > 0).map(_.value).reduceLeft(_ + _) / windowSize)
    val firstAvgLoss = TimeSerie(firstRSIDay,
      Math.abs(gainLoss.take(windowSize).filter(_.value < 0).map(_.value).reduceLeft(_ + _) / windowSize))
    val avgGain = avgGainLoss(windowSize, gainLoss, firstAvgGain, positive)
    val avgLoss = avgGainLoss(windowSize, gainLoss, firstAvgLoss, negative)
    avgGain.zip(avgLoss).map {
      case (gain, loss) => if(loss.value != 0) TimeSerie(gain.day, 100 - (100 / (1 + (gain.value/loss.value))))
      else TimeSerie(gain.day, 100)
    }
  }

  def takeHigh(tick:TickData):Double = tick.high
  def takeLow(tick:TickData):Double = tick.low
  def takePrice(tick:TickData):Double = tick.price
  def takeVolume(tick:TickData):Double = tick.volume
  def tsFromTick(ticks:List[TickData], selector:TickData => Double):List[TimeSerie] =
    ticks.map( tick => TimeSerie(tick.day, selector(tick)) )

  def stochasticOscillator(close:List[TickData], dayPeriod:Int, periodMA:Int):List[TimeSerie] = {
    close.sliding(dayPeriod).map { period =>
      val highestHigh = highest(tsFromTick(period, takeHigh), period.last.day.getMillis, dayPeriod)
      val lowestLow = lowest(tsFromTick(period, takeLow), period.last.day.getMillis, dayPeriod)
      TimeSerie(period.last.day, ((period.last.price-lowestLow.value)/(highestHigh.value-lowestLow.value))*100)
    }.toList
  }

  def laterOrEqualThan(list:List[TimeSerie], day:Long):List[TimeSerie] =
    list.filter(element => element.day.getMillis >= day)

  def flattenQuad(A:List[((TimeSerie,TimeSerie,TimeSerie),TimeSerie)]):List[(TimeSerie,TimeSerie,TimeSerie,TimeSerie)] =
    A.map( a => (a._1._1, a._1._2, a._1._3, a._2) )

  /**
   * marron = (rsi + mfi + BollOsc + (STOC / 3))/2
   */
  def computeMarron(RSI:List[TimeSerie], MFI:List[TimeSerie], BOSC:List[TimeSerie], STOC:List[TimeSerie],
                    firstDay:Long):List[TimeSerie] = {
    val rsi = laterOrEqualThan(RSI, firstDay)
    val mfi = laterOrEqualThan(MFI, firstDay)
    val bosc = laterOrEqualThan(BOSC, firstDay)
    val stoc = laterOrEqualThan(STOC, firstDay)
    val trio = (rsi, mfi, bosc).zipped
    val quad = flattenQuad( (trio, stoc).zipped.toList )
    quad.map( x => TimeSerie(x._1.day ,(x._1.value + x._2.value + x._3.value + (x._4.value/3))/2 ))
  }

  def computeVerde(marron:List[TimeSerie], oscp:List[TimeSerie], firstDay:Long):List[TimeSerie] = {
    val m = laterOrEqualThan(marron, firstDay)
    val o = laterOrEqualThan(oscp, firstDay)
    (m,o).zipped.toList.map( x => TimeSerie(x._1.day, (x._1.value + x._2.value)) )
  }

}






































