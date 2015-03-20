package com.ninthfloor.konkordator.indices

import com.github.nscala_time.time.Imports._

case class Tick(date: LocalDate, time: LocalTime, price: Double, volume: Double)

object Tick {

  private val Delimiter = ','

  def readCsv(line: String): Tick = {
    val Array(dateAndTime, price, volume, _) = line.split(Delimiter)
    val Array(date, time) = dateAndTime.split(' ')
    Tick(
      date = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(date).toLocalDate,
      time = DateTimeFormat.forPattern("HH:mm:ss").parseDateTime(time).toLocalTime,
      price = price.toDouble,
      volume = volume.toDouble
    )
  }
}
