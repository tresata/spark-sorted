package com.tresata.spark.sorted

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.reflect.ClassTag

import org.apache.spark.{ Partition, Partitioner, TaskContext, SparkEnv }
import org.apache.spark.rdd.RDD

/**
  * GroupSorted is a marker trait for key-value RDDs.
  * The contract for GroupSorted is as follows:
  * 1) all rows (key, value pairs) for a given key are consecutive and in the same partition
  * 2) the values can optionally be ordered per key
  */
trait GroupSorted[K, V] extends RDD[(K, V)] {
  def valueOrdering: Option[Ordering[V]]

  def mapStreamByKey[W: ClassTag](f: Iterator[V] => TraversableOnce[W]): RDD[(K, W)] =
    mapPartitions({ iter =>
      val biter = iter.buffered

      @tailrec
      def perKeyIterator(biter: BufferedIterator[(K, V)]): (Iterator[(K, W)], (Unit => Unit)) =
        if (biter.hasNext) {
          val k = biter.head._1

          val viter = new Iterator[V] {
            override def hasNext: Boolean = biter.hasNext && biter.head._1 == k

            override def next(): V = if (hasNext) biter.next()._2 else throw new NoSuchElementException("next on empty iterator")
          }

          val kwiter = f(viter).toIterator.map((k, _))
          val finish = { (_: Unit) => while (viter.hasNext) viter.next() }

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
          (Iterator.empty, identity)

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
    }, true)

  def foldLeftByKey[W: ClassTag](w: W)(f: (W, V) => W): RDD[(K, W)] = {
    // not-so-pretty stuff to serialize and deserialize w so it also works with mutable accumulators
    val wBuffer = SparkEnv.get.serializer.newInstance().serialize(w)
    val wArray = new Array[Byte](wBuffer.limit)
    wBuffer.get(wArray)
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance
    val wCreate = () => cachedSerializer.deserialize[W](ByteBuffer.wrap(wArray))

    mapStreamByKey(iter => Iterator(iter.foldLeft(wCreate())(f)))
  }
}

object GroupSorted {
  def apply[K, V](rdd: RDD[(K, V)], valueOrdering: Option[Ordering[V]]): GroupSorted[K, V] = {
    val vo = valueOrdering
    // there should be an easier way to do this
    new RDD[(K, V)](rdd) with GroupSorted[K, V] {
      override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = rdd.compute(split, context)

      override def getPartitions: Array[Partition] = rdd.partitions

      override val partitioner: Option[Partitioner] = rdd.partitioner

      override protected def getPreferredLocations(split: Partition): Seq[String] = rdd.preferredLocations(split)

      override def valueOrdering: Option[Ordering[V]] = vo
    }
  }
}
