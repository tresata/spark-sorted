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
        val result = mergeJoinIterators(aSorted.iterator, bSorted.iterator, bufferLeft).toList
        result === check
      }
    }

    it("should fail to merge-join 2 incorrectly sorted key-value iterators") {
      intercept[AssertionError](mergeJoinIterators(List((1, "a"), (2, "b"), (3, "c")).iterator, List((1, "a"), (3, "b"), (2, "c")).iterator, false).toList)
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
