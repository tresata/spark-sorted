package com.tresata.spark.sorted

import org.scalatest.FunSpec
import org.scalatest.prop.Checkers

class packageSpec extends FunSpec with Checkers {
  describe("mapStreamIterator") {
    it("should mapStream a key-value iterator") {
      check{ (l: List[(Int, Int)]) =>
        val nTake: Int => Int = i => i % 10

        val check = l
          .groupBy(_._1).mapValues(_.map(_._2).sorted).toList
          .flatMap{ case (k, vs) => vs.take(nTake(vs.head)).map((k, _)) }
          .sorted

        val result = mapStreamIterator(l.sorted.iterator){ iter =>
          val biter = iter.buffered
          biter.take(nTake(biter.head))
        }.toList
        result === check
      }
    }
  }

  describe("mergeJoinIterators") {
    it("should merge-join 2 sorted key-value iterators") {
      check{ (a: List[(Int, Int)], b: List[(Int, Int)], bufferLeft: Boolean) =>
        val aSorted = a.sortBy(_._1)
        val bSorted = b.sortBy(_._1)
        val check = {
          val aMap = a.groupBy(_._1).mapValues(_.map(_._2))
          val bMap = b.groupBy(_._1).mapValues(_.map(_._2))
          (aMap.keySet ++ bMap.keySet).toList.sorted.flatMap{ k =>
            (k, aMap.get(k), bMap.get(k)) match {
              case (k, Some(l1), Some(l2)) if bufferLeft =>
                for (v2 <- l2; v1 <- l1) yield (k, (Some(v1), Some(v2)))
              case (k, Some(l1), Some(l2)) =>
                for (v1 <- l1; v2 <- l2) yield (k, (Some(v1), Some(v2)))
              case (k, Some(l1), None) =>
                l1.map(v1 => (k, (Some(v1), None)))
              case (k, None, Some(l2)) =>
                l2.map(v2 => (k, (None, Some(v2))))
              case (k, None, None) =>
                sys.error("should never happen")
            }
          }
        }
        val f = if (bufferLeft) swapSides(fMergeJoinOuter[Int, Int]) else fMergeJoinOuter[Int, Int]
        val result = mergeJoinIterators(aSorted.iterator, bSorted.iterator, f, implicitly[Ordering[Int]]).toList
        result === check
      }
    }

    it("should fail to merge-join 2 incorrectly sorted key-value iterators") {
      intercept[AssertionError](mergeJoinIterators(List((1, "a"), (2, "b"), (3, "c")).iterator, List((1, "a"), (3, "b"), (2, "c")).iterator,
        fMergeJoinOuter[String, String], implicitly[Ordering[Int]]).toList)
    }

    it("should merge 2 sorted key-value iterators with a custom merge function") {
      check{ (a: List[(Int, Int)], b: List[(Int, Int)]) =>
        val aSorted = a.sortBy(_._1)
        val bSorted = b.sortBy(_._1)
        val f = { (it1: Iterator[Int], it2: Iterator[Int]) =>
          val l1 = it1.toList
          val l2 = it2.toList
          val nTake = (l1 ++ l2).head % 10
          (0 :: l1).take(nTake).flatMap{ v1 => (0 :: l2).take(nTake).map{ v2 => (v1, v2) } }
        }
        val check = {
          val aMap = a.groupBy(_._1).mapValues(_.map(_._2))
          val bMap = b.groupBy(_._1).mapValues(_.map(_._2))
          (aMap.keySet ++ bMap.keySet).toList.sorted.flatMap{ k =>
            val l1 = aMap.getOrElse(k, List.empty)
            val l2 = bMap.getOrElse(k, List.empty)
            val nTake = (l1 ++ l2).head % 10
            (0 :: l1).take(nTake).flatMap{ v1 => (0 :: l2).take(nTake).map{ v2 => (k, (v1, v2)) } }
          }
        }
        val result = mergeJoinIterators(aSorted.iterator, bSorted.iterator, f, implicitly[Ordering[Int]]).toList
        result === check
      }
    }
  }

  describe("mergeUnionIterators") {
    it("should merge-union 2 sorted iterators") {
      check{ (a: List[Int], b: List[Int]) =>
        val aSorted = a.sorted
        val bSorted = b.sorted
        val check = (a ++ b).sorted
        val result = mergeUnionIterators(aSorted.iterator, bSorted.iterator, Ordering.Int).toList
        result === check
      }
    }

    it("should fail to merge-union 2 incorrectly sorted iterators") {
      intercept[AssertionError](mergeUnionIterators(List(1, 2, 3).iterator, List(1, 3, 2).iterator, Ordering.Int).toList)
    }
  }
}
