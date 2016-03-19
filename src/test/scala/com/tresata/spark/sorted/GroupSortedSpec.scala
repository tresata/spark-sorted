package com.tresata.spark.sorted

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

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

    it("should groupSort on a GroupSorted without value ordering with no effect") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))

      val groupSorted = rdd.groupSort(new HashPartitioner(2), None)
      assert(groupSorted.groupSort(new HashPartitioner(2), None) eq groupSorted)
    }

    it("should groupSort on a GroupSorted with value ordering with no effect") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))

      val groupSorted = rdd.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]]))
      assert(groupSorted.groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]])) eq groupSorted)
    }

    it("should produce correctly partitioned key-value rdds without value ordering") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(new HashPartitioner(2), None)

        def collectPartition(p: Int): List[(Int, Int)] = {
          sc.runJob(rdd, (iter: Iterator[(Int, Int)]) => iter.toList, Seq(p)).head
        }

        rdd.partitioner.map{ partitioner =>
          partitioner.numPartitions == rdd.partitions.size && (0 to rdd.partitions.size - 1).forall{ i =>
            collectPartition(i).forall{ case (k, _) => partitioner.getPartition(k) == i }
          }
        }.getOrElse(false)
      })
    }

    it("should produce correctly partitioned key-value rdds with value ordering") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(new HashPartitioner(2), Some(implicitly[Ordering[Int]]))

        def collectPartition(p: Int): List[(Int, Int)] = {
          sc.runJob(rdd, (iter: Iterator[(Int, Int)]) => iter.toList, Seq(p)).head
        }

        rdd.partitioner.map{ partitioner =>
          partitioner.numPartitions == rdd.partitions.size && (0 to rdd.partitions.size - 1).forall{ i =>
            collectPartition(i).forall{ case (k, _) => partitioner.getPartition(k) == i }
          }
        }.getOrElse(false)
      })
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

    it("should reduceLeftByKey without value ordering") {
      val rdd = sc.parallelize(List(("c", Set("x")), ("a", Set("b")), ("a", Set("c")), ("b", Set("e")), ("b", Set("d"))))
      val sets = rdd.groupSort(new HashPartitioner(2), None).reduceLeftByKey { _ ++ _ }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should reduceLeftByKey with value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(new HashPartitioner(2), Some(Ordering.String)).reduceLeftByKey { _ + _ }
      assert(sets.collect.toMap === Map("a" -> "bc", "b" -> "de", "c" -> "x"))
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

    it("should scanLeftByKey with value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(new HashPartitioner(2), Some(Ordering.String)).scanLeftByKey(Set.empty[String]){ case (set, str) => set + str }
      assert(sets.collect.toSet === Set(
        ("a", Set()),
        ("a", Set("b")),
        ("a", Set("b", "c")),
        ("b", Set()),
        ("b", Set("d")),
        ("b", Set("d", "e")),
        ("c", Set()),
        ("c", Set("x"))
      ))
    }

    it("should mapStreamByKey with value ordering for randomly generated datasets and take operations") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val nTake: Int => Int = i => i % 10

        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(new HashPartitioner(2), implicitly[Ordering[Int]])
          .mapStreamByKey{ iter =>
            val biter = iter.buffered
            biter.take(nTake(biter.head))
          }
        val output = rdd.collect.toSet
        val check = input.filter(_._2.size > 0).flatMap{ case (k, l) =>
          val sorted = l.sorted
          sorted.take(nTake(sorted.head)).map(v => (k, v))
        }.toSet
        check === output
      })
    }

    it("should reduceLeftByKey without value ordering for randomly generated datasets") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(new HashPartitioner(2), None)
          .reduceLeftByKey(math.min)
        val output = rdd.collect.toSet
        val check = input.filter(_._2.size > 0).map{ case (k, l) => (k, l.reduce(math.min)) }.toSet
        check === output
      })
    }
  }
}
