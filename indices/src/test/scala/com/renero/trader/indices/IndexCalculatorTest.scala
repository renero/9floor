package com.renero.trader.indices

import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class IndexCalculatorTest extends FlatSpec with Matchers with PrivateMethodTester {

  "IndexCalculator" should "calculate mean" in {
    IndexCalculator.mean(List(9.4, 2.3, 10.2)) should be (7.3)
  }

  it should "return a mean of 0 on an empty list" in {
    IndexCalculator.mean(List()) should be (0.0)
  }
}
