package com.tresata.spark.sorted.sql

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.Checkers

import org.apache.spark.sql.{ Dataset, Encoder }
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import com.tresata.spark.sorted.SparkSuite
import PairDatasetFunctions._

case class TimeValue(time: Int, value: Double)

class GroupSortedDatasetSpec extends AnyFunSpec with Checkers with SparkSuite {
  import spark.implicits._

  def natOrd[V](reverse: Boolean)(implicit o: Ordering[V]): Ordering[V] = if (reverse) o.reverse else o

  def validGroupSorted[K: TypeTag, V: TypeTag, V1: Ordering](dataset: Dataset[(K, V)], reverse: Boolean, orderBy: V => V1): Boolean = {
    implicit val encoder: Encoder[Seq[(K, V)]] = ExpressionEncoder[Seq[(K, V)]]()
    val seq = dataset.mapPartitions(iter => Iterator(iter.toSeq)).collect().toSeq

    // check 1: no overlap in keys between partitions
    val check1 = seq.map(_.map(_._1).toSet).reduceOption(_ ++ _).map(_.size) == seq.map(_.map(_._1).toSet.size).reduceOption(_ + _)

    // check2 2: values are sorted per key
    val valueOrdering = natOrd[V1](reverse)
    val check2 = seq.flatten.sliding(2).forall{
      case Seq((k1, _), (k2, _)) if k1 != k2 => true
      case Seq(_) => true
      case Seq((k1, v1), (k2, v2)) if k1 == k2 => valueOrdering.compare(orderBy(v1), orderBy(v2)) <= 0
    }
    
    check1 && check2
  }

  def validGroupSorted[K: TypeTag, V: TypeTag: Ordering](dataset: Dataset[(K, V)], reverse: Boolean = false): Boolean = validGroupSorted[K, V, V](dataset, reverse, x => x)

  describe("PairDatasetFunctions") {
    it("should group-sort for empty dataset") {
      val seq = Seq.empty[(String, String)]
      val reverse = true

      val ds = seq.toDS()
        .groupSort(2, reverse)
        .toDS()
        .cache()

      validGroupSorted(ds, reverse) && ds === seq
    }

    it("should group-sort for randomly generated datasets") {
      check{ (seq: Seq[(String, String)], reverse: Boolean) =>
        val ds = seq.toDS()
          .groupSort(2, reverse)
          .toDS()
          .cache()

        validGroupSorted(ds, reverse) && ds === seq
      }
    }
  }

  describe("GroupSortedDataset") {
    it("should mapStreamByKey with take operations for randomly generated datasets") {
      check{ (seq: Seq[(Int, Int)], reverse: Boolean) =>
        val nTake: Int => Int = i => i % 10

        val ds = seq.toDS()
          .groupSort(2, reverse)
          .mapStreamByKey{ iter =>
          val biter = iter.buffered
          biter.take(nTake(biter.head))
        }
          .cache()

        val check = seq
          .groupBy(_._1).mapValues(_.map(_._2).sorted(natOrd[Int](reverse))).toSeq
          .flatMap{ case (k, vs) => vs.take(nTake(vs.head)).map((k, _)) }

        validGroupSorted(ds, reverse) && ds === check
      }
    }

    it("should mapStreamByKey with a mutable context") {
      val ds = Seq(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)).toDS()
      val withMax = ds
        .groupSort(2, true)
        .mapStreamByKey{ () => ArrayBuffer[Int]() }{ (buffer, iter) =>
          buffer.clear() // i hope this preserves the underlying array otherwise there is no point really in re-using it
          buffer ++= iter
          val max = buffer.head
          buffer.map(_ => max)
        }
        .cache()
      assert(validGroupSorted(withMax, true))
      assert(withMax ===  Seq(("a", 3), ("a", 3), ("b", 10), ("b", 10), ("c", 5)))
    }

    it("should foldLeftByKey") {
      val tseries = Seq(
        (5, TimeValue(2, 0.5)), (1, TimeValue(1, 1.2)), (5, TimeValue(1, 1.0)),
        (1, TimeValue(2, 2.0)), (1, TimeValue(3, 3.0))
      ).toDS()
      val emas = tseries
        .groupSort(2)
        .foldLeftByKey(0.0){ case (acc, TimeValue(time, value)) => 0.8 * acc + 0.2 * value }
        .cache()
      assert(validGroupSorted(emas))
      assert(emas === Seq((1, 1.0736), (5, 0.26)))
    }

    it("should reduceLeftByKey") {
      val ds = Seq(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")).toDS()
      val concat = ds
        .groupSort(2)
        .reduceLeftByKey { _ + _ }
        .cache()
      assert(validGroupSorted(concat))
      assert(concat === Seq("a" -> "bc", "b" -> "de", "c" -> "x"))
    }

    it("should mapStreamByKey while not exhausting iterators") {
      val ds = Seq(("a", 1), ("b", 10), ("a", 3), ("b", 1), ("c", 5)).toDS()
      val withMax = ds
        .groupSort(2, true)
        .mapStreamByKey{ iter => Iterator(iter.next()) }
        .cache()
      assert(validGroupSorted(withMax, true))
      assert(withMax ===  Seq(("a", 3), ("b", 10), ("c", 5)))
    }

    it("should mapStreamByKey if some keys have no output") {
      // see https://github.com/tresata/spark-sorted/issues/5
      val ds = Seq(("a", 1), ("c", 10), ("a", 3), ("c", 1), ("b", 5)).toDS()
      val filtered = ds
        .groupSort(2)
        .mapStreamByKey(_.filter(_ < 5))
        .cache()
      assert(validGroupSorted(filtered))
      assert(filtered ===  Seq(("a", 1), ("a", 3), ("c", 1)))
    }

    it("should scanLeftByKey") {
      val ds = Seq(("c", "x"), ("a", "b"), ("a", "c"), ("b", "e"), ("b", "d")).toDS()
      val seqs = ds
        .groupSort(2)
        .scanLeftByKey(Seq.empty[String]){ case (seq, str) => seq :+ str }
        .cache()

      // surprised this doesnt exist
      implicit def ord[X](implicit elemOrd: Ordering[X]): Ordering[Seq[X]] = new Ordering[Seq[X]] {
        def compare(x: Seq[X], y: Seq[X]): Int = {
          if (x.isEmpty) -1
          else if (y.isEmpty) 1
          else {
            val c = elemOrd.compare(x.head, y.head)
            if (c != 0)
              c
            else
              compare(x.tail, y.tail)
          }
        }
      }

      assert(validGroupSorted(seqs))
      assert(seqs === Seq(
        ("a", Seq()),
        ("a", Seq("b")),
        ("a", Seq("b", "c")),
        ("b", Seq()),
        ("b", Seq("d")),
        ("b", Seq("d", "e")),
        ("c", Seq()),
        ("c", Seq("x"))
      ))
    }

    it("should sort by function") {
      val ds = Seq(("a", ("b", Map("b" -> 1))), ("a", ("a", Map("a" -> 1)))).toDS()
      val x = ds
        .groupSort(2, reverse = true, sortBy = col => col("_1"))
        .mapStreamByKey(iter => iter)

      assert(validGroupSorted[String, (String, Map[String, Int]), String](x, true, v => v._1))
      assert(x === Seq(
        ("a", ("b", Map("b" -> 1))),
        ("a", ("a", Map("a" -> 1)))
      ))
    }
  }
}
