package com.tresata.spark.sorted

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.api.java.JavaSparkContext

object SparkSuite {
  lazy val sc = {
    val conf = new SparkConf(false)
      .setMaster("local")
      .setAppName("test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")
    new SparkContext(conf)
  }

  lazy val jsc = new JavaSparkContext(sc)
  def javaSparkContext() = jsc
}
