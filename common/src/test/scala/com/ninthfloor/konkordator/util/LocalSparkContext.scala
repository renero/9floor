package com.ninthfloor.konkordator.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest._

trait LocalSparkContext extends BeforeAndAfterAll {
  self: FlatSpec =>

  @transient var sc: SparkContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc = LocalSparkContext.getNewLocalSparkContext("test")
  }

  override def afterAll {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkContext {
  def getNewLocalSparkContext(title: String): SparkContext = new SparkContext("local", title)
}
