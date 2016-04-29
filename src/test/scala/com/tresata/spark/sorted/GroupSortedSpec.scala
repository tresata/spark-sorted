package com.tresata.spark.sorted

import org.scalatest.FunSpec
import org.scalatest.prop.Checkers
import org.scalacheck.{ Prop, Gen, Arbitrary }

case class TimeValue(time: Int, value: Double)

class GroupSortedSpec extends FunSpec with Checkers {
  lazy val sc = SparkSuite.sc

  import PairRDDFunctions._

  describe("PairRDDFunctions") {
    it("should groupSort on a RDD without value ordering") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))
      val groupSorted = rdd.groupSort(2)
      assert(groupSorted.glom.collect.map(_.map(_._1).toList).toSet === Set(List(2, 2), List(1, 1, 3))) // depends on hashcode
    }

    it("should groupSort on a RDD with value ordering") {
      val rdd = sc.parallelize(List((1, 2), (2, 3), (1, 3), (3, 1), (2, 1)))
      val groupSorted = rdd.groupSort(2, Ordering.Int)
      assert(groupSorted.glom.collect.map(_.toList).toSet ===  Set(List((2, 1), (2, 3)), List((1, 2), (1, 3), (3, 1)))) // depends on hashcode
    }

    it("should produce correctly partitioned key-value rdds without value ordering") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val input = l.zipWithIndex.map(_.swap)
        val rdd = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(2)

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
          .groupSort(2, Ordering.Int)

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

    it("should assign keys to same partitions sorted in same way regardless of value ordering") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val input = l.zipWithIndex.map(_.swap)
        val rdd1 = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(2)
        val rdd2 = sc.parallelize(input)
          .flatMapValues(identity)
          .groupSort(2, Ordering.Int)

        rdd1.glom.collect.toList.map(_.toList.map(_._1)) === rdd2.glom.collect.toList.map(_.toList.map(_._1))
        //collect.toList.map(_.toSet) === rdd2.glom.collect.toList.map(_.toSet)
      })
    }
  }

  describe("GroupSorted") {
    it("should mapStreamByKey without value ordering") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val sets = rdd.groupSort(2).mapStreamByKey(iter => Iterator(iter.toSet)) // very contrived...
      assert(sets.collect.toMap ===  Map("a" -> Set(1, 3), "b" -> Set(1, 10), "c" -> Set(5)))
    }

    it("should mapStreamByKey with value ordering") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val withMax = rdd.groupSort(2, Ordering.Int.reverse).mapStreamByKey{ iter =>
        val buffered = iter.buffered
        val max = buffered.head
        buffered.map(_ => max)
      }
      assert(withMax.collect.toList.groupBy(identity).mapValues(_.size) ===  Map(("a", 3) -> 2, ("b", 10) -> 2, ("c", 5) -> 1))
    }

    it("should foldLeftByKey without value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(2).foldLeftByKey(Set.empty[String]){ case (set, str) => set + str }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should foldLeftByKey with value ordering") {
      val ord = Ordering.by[TimeValue, Int](_.time)
      val tseries = sc.parallelize(List(
        (5, TimeValue(2, 0.5)), (1, TimeValue(1, 1.2)), (5, TimeValue(1, 1.0)),
        (1, TimeValue(2, 2.0)), (1, TimeValue(3, 3.0))
      ))
      val emas = tseries.groupSort(2, ord).foldLeftByKey(0.0){ case (acc, TimeValue(time, value)) => 0.8 * acc + 0.2 * value }
      assert(emas.collect.toSet === Set((1, 1.0736), (5, 0.26)))
    }

    it("should reduceLeftByKey without value ordering") {
      val rdd = sc.parallelize(List(("c", Set("x")), ("a", Set("b")), ("a", Set("c")), ("b", Set("e")), ("b", Set("d"))))
      val sets = rdd.groupSort(2).reduceLeftByKey { _ ++ _ }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should reduceLeftByKey with value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(2, Ordering.String).reduceLeftByKey { _ + _ }
      assert(sets.collect.toMap === Map("a" -> "bc", "b" -> "de", "c" -> "x"))
    }

    it("should mapStreamByKey with value ordering while not exhausting iterators") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val withMax = rdd.groupSort(2, Ordering.Int.reverse).mapStreamByKey{ iter =>
        Iterator(iter.next())
      }
      assert(withMax.collect.toSet ===  Set(("a", 3), ("b", 10), ("c", 5)))
    }

    it("should mapStreamByKey if some keys have no output") {
      // see https://github.com/tresata/spark-sorted/issues/5
      val rdd = sc.parallelize(List(("a", 1), ("c", 10), ("a", 3), ("c", 1), ("b", 5)))
      val filtered = rdd.groupSort(2, Ordering.Int.reverse).mapStreamByKey(_.filter(_ < 5))
      assert(filtered.collect.toSet ===  Set(("a", 1), ("a", 3), ("c", 1)))
    }

    it("should foldLeftByKey with a mutable accumulator") {
      import scala.collection.mutable.HashSet
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(2).foldLeftByKey(new HashSet[String]){ case (set, str) => set.add(str); set }
      assert(sets.collect.toMap === Map("a" -> Set("b", "c"), "b" -> Set("d", "e"), "c" -> Set("x")))
    }

    it("should scanLeftByKey with value ordering") {
      val rdd = sc.parallelize(List(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")))
      val sets = rdd.groupSort(2, Ordering.String).scanLeftByKey(Set.empty[String]){ case (set, str) => set + str }
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
          .groupSort(2, Ordering.Int)
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
          .groupSort(2)
          .reduceLeftByKey(math.min)
        val output = rdd.collect.toSet
        val check = input.filter(_._2.size > 0).map{ case (k, l) => (k, l.reduce(math.min)) }.toSet
        check === output
      })
    }

    it("should chain operations") {
      val rdd = sc.parallelize(List(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)))
      val withMax = rdd.groupSort(2, Ordering.Int.reverse).mapValues(_ + 1).mapStreamByKey{ iter =>
        val buffered = iter.buffered
        val max = buffered.head
        buffered.map(_ => max)
      }
      assert(withMax.collect.toList.groupBy(identity).mapValues(_.size) ===  Map(("a", 4) -> 2, ("b", 11) -> 2, ("c", 6) -> 1))
    }

    it("should merge join for test dataset") {
      val x = sc.parallelize((1 to 11).map(i => (i.toString, i.toString))).groupSort(1)
      val y = sc.parallelize((10 to 11).map(i => (i.toString, i.toString))).groupSort(1)
      assert(x.mergeJoinInner(y).collect.toSet === Set(("10", ("10", "10")), ("11", ("11", "11"))))
    }

    it("should merge join for randomly generated datasets") {
      val gen = Gen.containerOf[List, Int](Gen.choose(1, 100)).map(_.map(_.toString))
      val gen1 = for (l1 <- gen; l2 <- gen; l3 <- gen; l4 <- gen) yield (l1.zip(l2), l3.zip(l4))

      check(Prop.forAll(gen1){ case (a: List[(String, String)], b: List[(String, String)]) =>
        val check = {
          val aMap = a.groupBy(_._1).mapValues(_.map(_._2))
          val bMap = b.groupBy(_._1).mapValues(_.map(_._2))
          (aMap.keySet ++ bMap.keySet).toList.sorted.flatMap{ k =>
            (k, aMap.get(k), bMap.get(k)) match {
              case (k, Some(lv1), Some(lv2)) =>
                for (v1 <- lv1; v2 <- lv2) yield (k, (Some(v1), Some(v2)))
              case (k, Some(lv1), None) =>
                lv1.map(v1 => (k, (Some(v1), None)))
              case (k, None, Some(lv2)) =>
                lv2.map(v2 => (k, (None, Some(v2))))
              case (k, None, None) =>
                sys.error("should never happen")
            }
          }
        }

        val left = sc.parallelize(a).groupSort(2, Ordering.String)
        val right = sc.parallelize(b).groupSort(2, Ordering.String)

        val resultFullOuter = left.mergeJoin(right)
        val resultInner = left.mergeJoinInner(right)
        val resultLeftOuter = left.mergeJoinLeftOuter(right)
        val resultRightOuter = left.mergeJoinRightOuter(right)

        resultFullOuter.collect.toList.sorted === check.sorted
        resultInner.collect.toList.sorted === check.collect{ case (k, (Some(v), Some(w))) => (k, (v, w)) }.sorted
        resultLeftOuter.collect.toList.sorted === check.collect{ case (k, (Some(v), maybeW)) => (k, (v, maybeW)) }.sorted
        resultRightOuter.collect.toList.sorted === check.collect{ case (k, (maybeV, Some(w))) => (k, (maybeV, w)) }.sorted
      })
    }
  }
}
