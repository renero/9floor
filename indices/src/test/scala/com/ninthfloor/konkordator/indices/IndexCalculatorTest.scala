package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._
import com.ninthfloor.konkordator.util.LocalSparkContext
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class IndexCalculatorTest extends FlatSpec with Matchers with PrivateMethodTester with LocalSparkContext {

  private val TestDay = new LocalDate(2013, 1, 2)

  trait WithTickData {
    val ticks = sc.parallelize(Seq(
      Tick(TestDay, new LocalTime(17, 25, 19), 15.9350, 1950.0000),
      Tick(TestDay, new LocalTime(17, 25, 22), 15.9250, 73.0000),
      Tick(TestDay, new LocalTime(17, 25, 44), 15.9350, 1300.0000),
      Tick(TestDay + 1.days, new LocalTime(9, 57, 10), 15.9350, 0.0000),
      Tick(TestDay + 1.days, new LocalTime(9, 57, 10), 15.9300, 668.0000),
      Tick(TestDay + 2.days, new LocalTime(17, 29, 50), 15.9500, 103.0000),
      Tick(TestDay + 2.days, new LocalTime(17, 35, 27), 16.0000, 1276965.0000)
    ))
    val closingValues = Seq(
      ClosingValue(TestDay, 15.9350, 3323.0000, 15.9250, 15.9350),
      ClosingValue(TestDay + 1.days, 15.9300, 668.0000, 15.9300, 15.9350),
      ClosingValue(TestDay + 2.days, 16.0000, 1277068.0000, 15.9500, 16.0000)
    )
  }

  trait WithTimeSerie {
    val timeSerie = Seq(TimeSerie(TestDay, 3.0), TimeSerie(TestDay, 4.5), TimeSerie(TestDay, 6.0))
  }

  "IndexCalculator" should "filter indices" in new WithTimeSerie {
    IndexCalculator.filterIndices(timeSerie, TestDay, 2) should be (Seq(
      TimeSerie(TestDay, 6.0), TimeSerie(TestDay, 4.5)
    ))
  }

  it should "extract highest" in new WithTimeSerie {
    IndexCalculator.highest(timeSerie, TestDay, 2) should be (TimeSerie(TestDay, 6.0))
  }

  it should "extract lowest" in new WithTimeSerie {
    IndexCalculator.lowest(timeSerie, TestDay, 2) should be (TimeSerie(TestDay, 4.5))
  }

  it should "extract highest and lowest" in new WithTimeSerie {
    IndexCalculator.highestLowest(timeSerie, TestDay, 2) should be ((TimeSerie(TestDay, 6.0), TimeSerie(TestDay, 4.5)))
  }

  it should "calculate mean" in {
    IndexCalculator.mean(List(9.4, 2.3, 10.2)) should be (7.3)
  }

  it should "return a mean of 0 on an empty list" in {
    IndexCalculator.mean(List()) should be (0.0)
  }

  it should "calculate stddev" in {
    IndexCalculator.stddev(List(9.4, 2.3, 10.2)) should be (3.5505868059613284)
  }

  it should "return an stddev of 0 on an empty list" in {
    IndexCalculator.stddev(List()) should be (0.0)
  }

  it should "compute the closing values" in new WithTickData {
    IndexCalculator.closingValues(ticks) should be (Seq(
      ClosingValue(TestDay, 15.9350, 3323.0000, 15.9250, 15.9350),
      ClosingValue(TestDay + 1.days, 15.9300, 668.0000, 15.9300, 15.9350),
      ClosingValue(TestDay + 2.days, 16.0000, 1277068.0000, 15.9500, 16.0000)
    ))
  }

  it should "compute exponential average" in new WithTimeSerie {
    IndexCalculator.exponentialAverage(timeSerie, 0.1) should be (Seq(
      TimeSerie(TestDay, 3.0), TimeSerie(TestDay, 3.1500000000000004), TimeSerie(TestDay, 3.4350000000000005)
    ))
  }

  it should "computeTypical (money flow index)" in new WithTickData {
    IndexCalculator.computeTypical(closingValues) should be (Seq(
      TimeSerie(TestDay, 15.931666666666667), TimeSerie(TestDay + 1.days, 15.931666666666667),
      TimeSerie(TestDay + 2.days, 15.983333333333334)
    ))
  }

  it should "compute Negative Volume Index" in new WithTickData {
    IndexCalculator.computeVolumeIndex(closingValues,
      (today, yesterday) => today >= yesterday) should be (Seq(
        TimeSerie(TestDay, 1000.0), TimeSerie(TestDay + 1.days, 999.6862252902416),
        TimeSerie(TestDay + 2.days, 999.6862252902416)
    ))
  }

  it should "compute Positive Volume Index" in new WithTickData {
    IndexCalculator.computeVolumeIndex(closingValues,
      (today, yesterday) => today < yesterday) should be (Seq(
      TimeSerie(TestDay, 1000.0), TimeSerie(TestDay + 1.days, 1000.0),
      TimeSerie(TestDay + 2.days, 1004.3942247332078)
    ))
  }
}
