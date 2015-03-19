package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

import com.ninthfloor.konkordator.util.LocalSparkContext


class IndexCalculatorTest extends FlatSpec with Matchers with PrivateMethodTester with LocalSparkContext {

  private val TestDay = new DateTime(2013, 1, 2, 0, 0, 0)

  trait TickDataForTest {
    val midnight = new DateTime(1970, 1, 1, 23, 59, 59)
    val tickData = sc.parallelize(Seq(
      TickData(TestDay, new DateTime(0, 1, 1, 17, 25, 19), 15.9350, 1950.0000, 0, 0),
      TickData(TestDay, new DateTime(0, 1, 1, 17, 25, 22), 15.9250, 73.0000, 0, 0),
      TickData(TestDay, new DateTime(0, 1, 1, 17, 25, 44), 15.9350, 1300.0000, 0, 0),
      TickData(TestDay + 1.days, new DateTime(0, 1, 1, 9, 57, 10), 15.9350, 0.0000, 0, 0),
      TickData(TestDay + 1.days, new DateTime(0, 1, 1, 9, 57, 10), 15.9300, 668.0000, 0, 0),
      TickData(TestDay + 2.days, new DateTime(0, 1, 1, 17, 29, 50), 15.9500, 103.0000, 0, 0),
      TickData(TestDay + 2.days, new DateTime(0, 1, 1, 17, 35, 27), 16.0000, 1276965.0000, 0, 0)
    ))

    val tickClosingValuesData = Seq(
      TickData(TestDay, midnight, 15.9350, 3323.0000, 15.9250, 15.9350),
      TickData(TestDay + 1.days, midnight, 15.9300, 668.0000, 15.9300, 15.9350),
      TickData(TestDay + 2.days, midnight, 16.0000, 1277068.0000, 15.9500, 16.0000)
    )
  }

  trait TimeSerieForTest {
    val timeSerie = Seq(TimeSerie(TestDay, 3.0), TimeSerie(TestDay, 4.5), TimeSerie(TestDay, 6.0))
  }

  "IndexCalculator" should "calculate mean" in {
    IndexCalculator.mean(List(9.4, 2.3, 10.2)) should be (7.3)
  }

  it should "return a mean of 0 on an empty list" in {
    IndexCalculator.mean(List()) should be (0.0)
  }

  it should "compute the closing values" in new TickDataForTest {
    IndexCalculator.closingValues(tickData) should be (Seq(
      TickData(TestDay, midnight, 15.9350, 3323.0000, 15.9250, 15.9350),
      TickData(TestDay + 1.days, midnight, 15.9300, 668.0000, 15.9300, 15.9350),
      TickData(TestDay + 2.days, midnight, 16.0000, 1277068.0000, 15.9500, 16.0000)
    ))
  }

  it should "compute exponential average" in new TimeSerieForTest {
    IndexCalculator.exponentialAverage(timeSerie, 0.1) should be (Seq(
      TimeSerie(TestDay, 3.0), TimeSerie(TestDay, 3.1500000000000004), TimeSerie(TestDay, 3.4350000000000005)
    ))
  }

  it should "computeTypical (money flow index)" in new TickDataForTest {
    IndexCalculator.computeTypical(tickClosingValuesData) should be (Seq(
      TimeSerie(TestDay, 15.931666666666667), TimeSerie(TestDay + 1.days, 15.931666666666667),
      TimeSerie(TestDay + 2.days, 15.983333333333334)
    ))
  }

  it should "compute Negative Volume Index" in new TickDataForTest {
    IndexCalculator.computeVolumeIndex(tickClosingValuesData,
      (today, yesterday) => today >= yesterday) should be (Seq(
        TimeSerie(TestDay, 1000.0), TimeSerie(TestDay + 1.days, 999.6862252902416),
        TimeSerie(TestDay + 2.days, 999.6862252902416)
    ))
  }

  it should "compute Positive Volume Index" in new TickDataForTest {
    IndexCalculator.computeVolumeIndex(tickClosingValuesData,
      (today, yesterday) => today < yesterday) should be (Seq(
      TimeSerie(TestDay, 1000.0), TimeSerie(TestDay + 1.days, 1000.0),
      TimeSerie(TestDay + 2.days, 1004.3942247332078)
    ))
  }
}
