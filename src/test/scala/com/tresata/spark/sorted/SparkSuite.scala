package com.tresata.spark.sorted

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaSparkContext

import org.scalactic.Equality

object SparkSuite {
  lazy val sc = {
    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")
    new SparkContext(conf)
  }

  lazy val jsc = new JavaSparkContext(sc)
  def javaSparkContext() = jsc
}

trait SparkSuite {
  lazy val sc = SparkSuite.sc

  implicit def rddEq[X] = new Equality[RDD[X]] {
    private def toCounts[Y](t: TraversableOnce[Y]): Map[Y, Int] = t.toSeq.groupBy(identity).mapValues(_.size)

    def areEqual(a: RDD[X], b: Any): Boolean = b match {
      case s: TraversableOnce[_] => toCounts(a.collect) == toCounts(s)
      case rdd: RDD[_] => toCounts(a.collect) == toCounts(rdd.collect)
    }
  }

  implicit def gsEq[K, V](implicit rddEq: Equality[RDD[(K, V)]]) = new Equality[GroupSorted[K, V]] {
    def areEqual(a: GroupSorted[K, V], b: Any): Boolean = rddEq.areEqual(a, b)
  }
}
