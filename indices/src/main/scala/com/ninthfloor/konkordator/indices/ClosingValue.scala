package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._

case class ClosingValue(date: LocalDate, price: Double, volume: Double, low: Double, high: Double)
