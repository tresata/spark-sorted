package com.tresata.spark.sorted

import org.apache.spark.HashPartitioner

import org.scalatest.FunSpec
import org.scalatest.prop.Checkers
import org.scalacheck.{ Prop, Arbitrary }

case class TimeValue(time: Int, value: Double)

class GroupSortedSpec extends FunSpec with Checkers {
  lazy val sc = SparkSuite.sc

  import PairRDDFunctions._

  describe("PairRDDFunctions") {
    it("should groupSort on a RDD without value ordering") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))
      val groupSorted = rdd.groupSort(new HashPartitioner(2), None)
      assert(groupSorted.map(_._1).collect === List(2, 2, 1, 1, 3)) // ordering depends on hashcode!
      assert(groupSorted.glom.collect.map(_.map(_._1).toList).toSet === Set(List(2, 2), List(1, 1, 3)))
    }

    it("should groupSort on a RDD with value ordering") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))
      val groupSorted = rdd.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]]))
      assert(groupSorted.map(_._1).collect === List(2, 2, 1, 1, 3)) // ordering depends on hashcode!
      assert(groupSorted.glom.collect.map(_.toList).toSet ===  Set(List((2, 1), (2, 3)), List((1, 2), (1, 3), (3, 1))))
    }

    it("should groupSort on a GroupSorted with no effect") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))
      val groupSorted = rdd.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]]))
      assert(groupSorted.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]])) eq groupSorted)
    }
  }

  describe("GroupSorted") {
    it("should mapStreamByKey without value ordering") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val sets = rdd.groupSort(new HashPartitioner(2), None).mapStreamByKey(iter => Iterator(iter.toSet)) // very contrived...
      assert(sets.collect.toMap ===  Map("a" -> Set(1, 3), "b" -> Set(1, 10), "c" -> Set(5)))
    }

    it("should mapStreamByKey with value ordering") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val withMax = rdd.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]].reverse)).mapStreamByKey{ iter =>
        val buffered = iter.buffered
        val max = buffered.head
        buffered.map(_ => max)
      }
      assert(withMax.collect.toList.groupBy(identity).mapValues(_.size) ===  Map(("a", 3) -> 2, ("b", 10) -> 2, ("c", 5) -> 1))
    }

    it("should foldLeftByKey without value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(new HashPartitioner(2), None).foldLeftByKey(Set.empty[String]){ case (set, str) => set + str }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should foldLeftByKey with value ordering") {
      val ord = Ordering.by[TimeValue, Int](_.time)
      val tseries = sc.parallelize(List(
        (5, TimeValue(2, 0.5)), (1, TimeValue(1, 1.2)), (5, TimeValue(1, 1.0)),
        (1, TimeValue(2, 2.0)), (1, TimeValue(3, 3.0))
      ))
      val emas = tseries.groupSort(new HashPartitioner(2), Some(ord)).foldLeftByKey(0.0){ case (acc, TimeValue(time, value)) => 0.8 * acc + 0.2 * value }
      assert(emas.collect.toSet === Set((1, 1.0736), (5, 0.26)))
    }

    it("should mapStreamByKey with value ordering while not exhausting iterators") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val withMax = rdd.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]].reverse)).mapStreamByKey{ iter =>
        Iterator(iter.next())
      }
      assert(withMax.collect.toSet ===  Set(("a", 3), ("b", 10), ("c", 5)))
    }

    it("should mapStreamByKey if some keys have no output") {
      // see https://github.com/tresata/spark-sorted/issues/5
      val rdd = sc.parallelize(List(("a", 1), ("c", 10), ("a", 3), ("c", 1), ("b", 5)))
      val filtered = rdd.groupSort(new HashPartitioner(1), Some(implicitly[Ordering[Int]].reverse)).mapStreamByKey(_.filter(_ < 5))
      assert(filtered.collect.toSet ===  Set(("a", 1), ("a", 3), ("c", 1)))
    }

    it("should foldLeftByKey with a mutable accumulator") {
      import scala.collection.mutable.HashSet
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(new HashPartitioner(2), None).foldLeftByKey(new HashSet[String]){ case (set, str) => set.add(str); set }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should mapStreamByKey for many randomly generated datasets and take operations") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val nTake: List[Int] => Int = _.headOption.getOrElse(0).hashCode % 10

        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(new HashPartitioner(1), implicitly[Ordering[Int]])
          .mapStreamByKey{ iter =>
            val l = iter.toList
            l.take(nTake(l))
          }
        val output = rdd.collect.toSet
        val check = input.flatMap{ case (k, l) =>
          val sorted = l.sorted
          sorted.take(nTake(sorted)).map(v => (k, v))
        }.toSet
        check === output
      })
    }
  }
}
