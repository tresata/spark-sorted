package com.tresata.spark.sorted

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuilder

object `package` {
  // assumes all values for a given key are consecutive
  private[sorted] def mapStreamIteratorWithContext[K, V, W, C](iter: Iterator[(K, V)])(c: () => C, f: (C, Iterator[V]) => TraversableOnce[W]): Iterator[(K, W)] = {
    val context = c()
    val biter = iter.buffered

    @tailrec
    def perKeyIterator(biter: BufferedIterator[(K, V)]): (Iterator[(K, W)], (() => Unit)) =
      if (biter.hasNext) {
        val k = biter.head._1

        val viter = new Iterator[V] {
          override def hasNext: Boolean = biter.hasNext && biter.head._1 == k

          override def next(): V = if (hasNext) biter.next()._2 else throw new NoSuchElementException("next on empty iterator")
        }

        val kwiter = f(context, viter).toIterator.map((k, _))
        val finish = { () => while (viter.hasNext) viter.next() }

        if (kwiter.hasNext)
          (kwiter, finish)
        else {
          // see https://github.com/tresata/spark-sorted/issues/5
          // if the returned iterator does not have any values next keys will not get processed
          // the solution is to never return this iterator but move on to the next one
          finish() // make sure underlying iterator is exhausted
          perKeyIterator(biter)
        }
      } else
        (Iterator.empty, () => ())

    new Iterator[(K, W)] {
      private var (kwiter, finish) = perKeyIterator(biter)

      override def hasNext: Boolean = {
        if (!kwiter.hasNext) {
          finish() // make sure underlying iterator is exhausted
          val tmp = perKeyIterator(biter); kwiter = tmp._1; finish = tmp._2 // roll to next iterator
        }
        kwiter.hasNext
      }

      override def next: (K, W) = if (hasNext) kwiter.next() else throw new NoSuchElementException("next on empty iterator")
    }
  }

  private[sorted] def mapStreamIterator[K, V, W](iter: Iterator[(K, V)])(f: Iterator[V] => TraversableOnce[W]): Iterator[(K, W)] =
    mapStreamIteratorWithContext[K, V, W, Unit](iter)(() => (), (_: Unit, it: Iterator[V]) => f(it))

  // assumes both iterators are sorted by key with repeat keys allowed
  // key cannot be null
  private def mergeJoinIterators[K, V1, V2: ClassTag](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], ord: Ordering[K]): Iterator[(K, (Option[V1], Option[V2]))] =
    new Iterator[(Option[(K, V1)], Option[(K, Array[V2])])] {
      private val bit1 = it1.buffered
      private val bit2 = it2.buffered

      private var prevK1: K = _
      private var prevK2: K = _
      private val v2sBuilder = ArrayBuilder.make[V2]

      def hasNext: Boolean = bit1.hasNext || bit2.hasNext

      def next: (Option[(K, V1)], Option[(K, Array[V2])]) = {
        val hasNext1 = bit1.hasNext
        val hasNext2 = bit2.hasNext
        val comp = if (hasNext1 && hasNext2) ord.compare(bit1.head._1, bit2.head._1) else 0
        val maybeK1V1 = if (hasNext1 && (!hasNext2 || comp <= 0)) {
          val (k1, v1) = bit1.next()
          assert(prevK1 == null || ord.compare(prevK1, k1) <= 0) // repeat keys are ok
          prevK1 = k1
          Some((k1, v1))
        } else None
        val maybeK2Vs2 = if (prevK1 == prevK2 && maybeK1V1.map(_._1 == prevK1).getOrElse(false)) {
          Some((prevK2, v2sBuilder.result))
        } else if (hasNext2 && (!hasNext1 || comp >= 0)) {
          val k2 = bit2.head._1
          assert(prevK2 == null || ord.compare(prevK2, k2) < 0) // repeat keys are not ok
          prevK2 = k2
          v2sBuilder.clear()
          while (bit2.hasNext && bit2.head._1 == k2) // for given key all values on right side must fit in memory
            v2sBuilder += bit2.next()._2
          Some((k2, v2sBuilder.result))
        } else None
        (maybeK1V1, maybeK2Vs2)
      }
    }.flatMap {
      case (Some((k1, v1)), Some((k2, v2s))) =>
        assert(k1 == k2)
        v2s.map(v2 => (k1, (Some(v1), Some(v2))))
      case (Some((k1, v1)), None) =>
        Iterator.single((k1, (Some(v1), None)))
      case (None, Some((k2, v2s))) =>
        v2s.map(v2 => (k2, (None, Some(v2))))
      case (None, None) =>
        throw new NoSuchElementException("next on empty iterator")
    }

  private[sorted] def mergeJoinIterators[K, V1: ClassTag, V2: ClassTag](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], ord: Ordering[K], bufferLeft: Boolean):
      Iterator[(K, (Option[V1], Option[V2]))] =
    if (bufferLeft)
      mergeJoinIterators(it2, it1, ord).map(kv => (kv._1, kv._2.swap))
    else
      mergeJoinIterators(it1, it2, ord)

  private[sorted] def mergeJoinIterators[K, V1, V2](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], bufferLeft: Boolean)(
    implicit ord: Ordering[K], ctv1: ClassTag[V1], ctv2: ClassTag[V2]): Iterator[(K, (Option[V1], Option[V2]))] = mergeJoinIterators(it1, it2, ord, bufferLeft)

  // assumes both iterators are sorted
  // is safe with a partial ordering
  private[sorted] def mergeUnionIterators[X](it1: Iterator[X], it2: Iterator[X], ord: Ordering[X]): Iterator[X] = new Iterator[X] {
    private val bit1 = it1.buffered
    private val bit2 = it2.buffered

    // trust but verify: we cannot be sure that correct ordering was used for the datasets so we check it
    private var prev1: X = _
    private var prev2: X = _

    def hasNext: Boolean = bit1.hasNext || bit2.hasNext

    def next(): X = {
      val hasNext1 = bit1.hasNext
      val hasNext2 = bit2.hasNext
      val negComp = hasNext1 && hasNext2 && ord.compare(bit1.head, bit2.head) <= 0
      if (hasNext1 && (!hasNext2 || negComp)) {
        val x1 = bit1.next()
        assert(prev1 == null || ord.compare(prev1, x1) <= 0)
        prev1 = x1
        x1
      } else {
        val x2 = bit2.next()
        assert(prev2 == null || ord.compare(prev2, x2) <= 0)
        prev2 = x2
        x2
      }
    }
  }
}
