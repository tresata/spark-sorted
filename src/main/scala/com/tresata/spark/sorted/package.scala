package com.tresata.spark.sorted

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv

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

      override def next(): (K, W) = if (hasNext) kwiter.next() else throw new NoSuchElementException("next on empty iterator")
    }
  }

  private[sorted] def mapStreamIterator[K, V, W](iter: Iterator[(K, V)])(f: Iterator[V] => TraversableOnce[W]): Iterator[(K, W)] =
    mapStreamIteratorWithContext[K, V, W, Unit](iter)(() => (), (_: Unit, it: Iterator[V]) => f(it))

  // assumes both iterators are sorted by key with repeat keys allowed
  // key cannot be null
  private[sorted] def mergeJoinIterators[K, V1, V2, W](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], f: (Iterator[V1], Iterator[V2]) => TraversableOnce[W], ord: Ordering[K]): Iterator[(K, W)] = {
    def iterForKey[V](key: K, bit: BufferedIterator[(K, V)]): Iterator[V] = new Iterator[V]{
      override def hasNext: Boolean = bit.hasNext && bit.head._1 == key

      override def next: V = if (hasNext) bit.next._2 else throw new NoSuchElementException("next on empty iterator")
    }

    new Iterator[(K, W)] {
      private val bit1 = it1.buffered
      private val bit2 = it2.buffered

      private def nextKeyIter(): (K, Iterator[W]) = {
        def nextIter(key: K): Iterator[W] = f(iterForKey(key, bit1), iterForKey(key, bit2)).toIterator

        val hasNext1 = bit1.hasNext
        val hasNext2 = bit2.hasNext

        if (hasNext1 || hasNext2) {
          val key = if (hasNext1 && hasNext2) {
            val key1 = bit1.head._1
            val key2 = bit2.head._1
            if (ord.compare(key1, key2) <= 0) key1 else key2
          } else if (hasNext1)
            bit1.head._1
          else
            bit2.head._1
          (key, nextIter(key)) 
        } else null
      }

      private var prevKey: K = _
      private var currKeyIter = nextKeyIter()

      private def update(): Unit = {
        while (currKeyIter != null && !currKeyIter._2.hasNext) {
          while (bit1.hasNext && bit1.head._1 == currKeyIter._1)
            bit1.next()
          while (bit2.hasNext && bit2.head._1 == currKeyIter._1)
            bit2.next()
          prevKey = currKeyIter._1
          currKeyIter = nextKeyIter()
          assert(currKeyIter == null || ord.compare(prevKey, currKeyIter._1) < 0)
        }
      }

      override def hasNext: Boolean = {
        update()
        currKeyIter != null && currKeyIter._2.hasNext
      }

      override def next(): (K, W) =
        if (hasNext)
          (currKeyIter._1, currKeyIter._2.next())
        else
          throw new NoSuchElementException("next on empty iterator")
    }
  }

  private[sorted] def mergeJoinIterators[K, V1, V2, W](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], f: (Iterator[V1], Iterator[V2]) => TraversableOnce[W])(implicit ord1: Ordering[K],
    dummy: DummyImplicit): Iterator[(K, W)] = mergeJoinIterators[K, V1, V2, W](it1, it2, f, ord1)
  
  private def mergeJoinIterators[K, V1, V2: ClassTag](it1: Iterator[(K, V1)], it2: Iterator[(K, V2)], ord: Ordering[K]): Iterator[(K, (Option[V1], Option[V2]))] = {
    val f: (Iterator[V1], Iterator[V2]) => TraversableOnce[(Option[V1] ,Option[V2])] = { (it1, it2) =>
      val a2 = it2.toArray
      if (it1.hasNext) {
        if (a2.isEmpty)
          it1.map{ v1 => (Some(v1), None) }
        else
          it1.flatMap{ v1 => a2.map(v2 => (Some(v1), Some(v2))) }
      } else {
        a2.map{ v2 => (None, Some(v2)) }
      }
    }
    mergeJoinIterators(it1, it2, f, ord)
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

    override def hasNext: Boolean = bit1.hasNext || bit2.hasNext

    override def next(): X = {
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

  private[sorted] def newWCreate[W: ClassTag](w: W): () => W = {
    // not-so-pretty stuff to serialize and deserialize w so it also works with mutable accumulators
    val wBuffer = SparkEnv.get.serializer.newInstance().serialize(w)
    val wArray = new Array[Byte](wBuffer.limit)
    wBuffer.get(wArray)
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance
    () => cachedSerializer.deserialize[W](ByteBuffer.wrap(wArray))
  }
}
