package com.tresata.spark.sorted

import org.scalatest.FunSpec
import org.scalatest.prop.Checkers
import org.scalacheck.{ Prop, Gen, Arbitrary }

class packageSpec extends FunSpec with Checkers {
  describe("mapStreamIterator") {
    it("should mapStream a key-value iterator") {
      val gen = Arbitrary.arbitrary[List[List[Int]]]

      check(Prop.forAll(gen){ l =>
        val nTake: Int => Int = i => i % 10

        val input = l.zipWithIndex.map(_.swap)
        val check = input.filter(_._2.size > 0).flatMap{ case (k, vs) =>
          val sorted = vs.sorted
          sorted.take(nTake(sorted.head)).map(v => (k, v))
        }.toSet
        val iter = input.flatMap{ case (k, vs) => vs.sorted.map(v => (k, v)) }.iterator
        val result = mapStreamIterator(iter){ iter =>
          val biter = iter.buffered
          biter.take(nTake(biter.head))
        }.toSet
        result === check
      })
    }
  }

  describe("mergeJoinIterators") {
    it("should merge-join 2 sorted key-value iterators") {
      val gen = Gen.containerOf[List, Int](Gen.choose(1, 100))
      val gen1 = Arbitrary.arbitrary[Boolean]
      val gen2 = for (l1 <- gen; l2 <- gen; l3 <- gen; l4 <- gen; b <- gen1) yield (l1.zip(l2), l3.zip(l4), b)

      check(Prop.forAll(gen2){ case (a: List[(Int, Int)], b: List[(Int, Int)], bufferLeft) =>
        val aSorted = a.sortBy(_._1)
        val bSorted = b.sortBy(_._1)
        val check = {
          val aMap = a.groupBy(_._1).mapValues(_.map(_._2))
          val bMap = b.groupBy(_._1).mapValues(_.map(_._2))
          (aMap.keySet ++ bMap.keySet).toList.sorted.flatMap{ k =>
            (k, aMap.get(k), bMap.get(k)) match {
              case (k, Some(lv1), Some(lv2)) if bufferLeft =>
                for (v2 <- lv2; v1 <- lv1) yield (k, (Some(v1), Some(v2)))
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
        val result = mergeJoinIterators(aSorted.iterator, bSorted.iterator, bufferLeft).toList
        result == check
      })
    }

    it("should fail to merge-join 2 incorrectly sorted key-value iterators") {
      intercept[AssertionError](mergeJoinIterators(List((1, "a"), (2, "b"), (3, "c")).iterator, List((1, "a"), (3, "b"), (2, "c")).iterator, false).toList)
    }
  }
}
