package com.tresata.spark.sorted

import org.scalactic.Equality
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }

object SparkSuite {
  lazy val spark: SparkSession = {
    val session = SparkSession.builder
      .master("local[*]")
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.enabled", false)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    session
  }
  lazy val sc: SparkContext = spark.sparkContext

  lazy val jsc = new JavaSparkContext(sc)
  def javaSparkContext() = jsc
}

trait SparkSuite {
  implicit lazy val spark: SparkSession = SparkSuite.spark
  implicit lazy val sc: SparkContext = SparkSuite.spark.sparkContext

  implicit def rddEq[X]: Equality[RDD[X]] = new Equality[RDD[X]] {
    private def toCounts[Y](s: Seq[Y]): Map[Y, Int] = s.groupBy(identity).mapValues(_.size)

    def areEqual(a: RDD[X], b: Any): Boolean = b match {
      case s: Seq[_] => toCounts(a.collect) == toCounts(s)
      case rdd: RDD[_] => toCounts(a.collect) == toCounts(rdd.collect)
    }
  }

  implicit def gsEq[K, V](implicit rddEq: Equality[RDD[(K, V)]]): Equality[GroupSorted[K, V]] = new Equality[GroupSorted[K, V]] {
    def areEqual(a: GroupSorted[K, V], b: Any): Boolean = rddEq.areEqual(a, b)
  }
  
  implicit def dsEq[X](implicit rddEq: Equality[RDD[X]]): Equality[Dataset[X]] = new Equality[Dataset[X]] {
    def areEqual(a: Dataset[X], b: Any): Boolean = b match {
      case ds: Dataset[_] => rddEq.areEqual(a.rdd, ds.rdd)
      case x => rddEq.areEqual(a.rdd, x)
    }
  }
}
