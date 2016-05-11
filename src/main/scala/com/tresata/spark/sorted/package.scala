package com.tresata.spark.sorted

import scala.annotation.tailrec
import scala.reflect.ClassTag

object `package` {
  // assumes all values for a given key are consecutive
  private[sorted] def mapStreamIterator[K, V, W](iter: Iterator[(K, V)])(f: Iterator[V] => TraversableOnce[W]): Iterator[(K, W)] = {
    val biter = iter.buffered

    @tailrec
    def perKeyIterator(biter: BufferedIterator[(K, V)]): (Iterator[(K, W)], (() => Unit)) =
      if (biter.hasNext) {
        val k = biter.head._1

        val viter = new Iterator[V] {
          override def hasNext: Boolean = biter.hasNext && biter.head._1 == k

          override def next(): V = if (hasNext) biter.next()._2 else throw new NoSuchElementException("next on empty iterator")
        }

        val kwiter = f(viter).toIterator.map((k, _))
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

  // assumes both iterators are sorted by key with repeat keys allowed
  // key cannot be null
  private[sorted] def mergeJoinIterators[K, V1, V2](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], ord: Ordering[K], bufferLeft: Boolean)(
    implicit ctv1: ClassTag[V1], ctv2: ClassTag[V2]): Iterator[(K, (Option[V1], Option[V2]))] =
    new Iterator[(K, Option[Iterator[V1]], Option[Iterator[V2]])] {
      private def collapseRepeats[K, V](it: Iterator[(K, V)]): Iterator[(K, Iterator[V])] = new Iterator[(K, Iterator[V])] {
        private val buf = it.buffered

        def hasNext: Boolean = buf.hasNext

        def next: (K, Iterator[V]) = {
          val k = buf.head._1
          val vit = new Iterator[V] {
            def hasNext: Boolean = buf.hasNext && buf.head._1 == k
            def next: V = buf.next._2
          }
          (k, vit)
        }
      }
  
      private val bcit1 = collapseRepeats(it1).buffered
      private val bcit2 = collapseRepeats(it2).buffered

      // trust but verify: we cannot be sure that correct ordering was used for the datasets so we check it
      private var prevK1: K = _
      private var prevK2: K = _

      def hasNext: Boolean = bcit1.hasNext || bcit2.hasNext

      def next: (K, Option[Iterator[V1]], Option[Iterator[V2]]) =
        if (bcit1.hasNext && bcit2.hasNext) {
          val comp = ord.compare(bcit1.head._1, bcit2.head._1)
          if (comp < 0) {
            val (k1, itv1) = bcit1.next
            assert(prevK1 == null || ord.compare(prevK1, k1) < 0)
            prevK1 = k1
            (k1, Some(itv1), None)
          } else if (comp > 0) {
            val (k2, itv2) = bcit2.next
            assert(prevK2 == null || ord.compare(prevK2, k2) < 0)
            prevK2 = k2
            (k2, None, Some(itv2))
          } else {
            val (k1, itv1) = bcit1.next
            assert(prevK1 == null || ord.compare(prevK1, k1) < 0)
            prevK1 = k1
            val (k2, itv2) = bcit2.next
            assert(prevK2 == null || ord.compare(prevK2, k2) < 0)
            prevK2 = k2
            assert(k1 == k2)
            (k1, Some(itv1), Some(itv2))
          }
        } else if (bcit1.hasNext) {
          val (k1, itv1) = bcit1.next
          assert(prevK1 == null || ord.compare(prevK1, k1) < 0)
          prevK1 = k1
          (k1, Some(itv1), None)
        } else {
          val (k2, itv2) = bcit2.next
          assert(prevK2 == null || ord.compare(prevK2, k2) < 0)
          prevK2 = k2
          (k2, None, Some(itv2))
        }
    }.flatMap{
      case (k, Some(itv1), None) =>
        itv1.map(v1 => (k, (Some(v1), None)))
      case (k, None, Some(itv2)) =>
        itv2.map(v2 => (k, (None, Some(v2))))
      case (k, Some(itv1), Some(itv2)) =>
        if (bufferLeft) {
          val av1 = itv1.toArray // all values on left side must fit in memory
          itv2.flatMap{ v2 =>
            av1.map(v1 => (k, (Some(v1), Some(v2))))
          }
        } else {
          val av2 = itv2.toArray // all values on right side must fit in memory
          itv1.flatMap{ v1 =>
            av2.map(v2 => (k, (Some(v1), Some(v2))))
          }
        }
      case (k, None, None) =>
        sys.error("this should never happen")
    }

  private[sorted] def mergeJoinIterators[K, V1, V2](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], bufferLeft: Boolean)(
    implicit ord: Ordering[K], ctv1: ClassTag[V1], ctv2: ClassTag[V2]): Iterator[(K, (Option[V1], Option[V2]))] = mergeJoinIterators(it1, it2, ord, bufferLeft)
}
