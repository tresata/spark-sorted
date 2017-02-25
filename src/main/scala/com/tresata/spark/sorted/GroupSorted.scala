package com.tresata.spark.sorted

import scala.reflect.ClassTag

import org.apache.spark.{ Aggregator, Partition, Partitioner, TaskContext }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.{ RDD, ShuffledRDD }

/**
  * GroupSorted is a partitioned key-value RDD where the values for a given key are consecutive and within a single partition.
  * The keys are sorted within a partition (using a custom hash based Ordering). The values can also be optionally sorted per key.
  */
class GroupSorted[K, V] private (rdd: RDD[(K, V)], val keyOrdering: Ordering[K], val valueOrdering: Option[Ordering[V]])(
  implicit kClassTag: ClassTag[K], vClassTag: ClassTag[V]) extends RDD[(K, V)](rdd) {
  assert(rdd.partitioner.isDefined)

  private def pair = new org.apache.spark.rdd.PairRDDFunctions(this)

  private def copy[W: ClassTag](rdd1: RDD[(K, W)], valueOrdering1: Option[Ordering[W]]): GroupSorted[K, W] = new GroupSorted(rdd1, keyOrdering, valueOrdering1)

  // overrides for RDD

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = firstParent[(K, V)].compute(split, context)

  override def getPartitions: Array[Partition] = firstParent[(K, V)].partitions

  override val partitioner: Option[Partitioner] = firstParent[(K, V)].partitioner

  override def filter(f: ((K, V)) => Boolean): GroupSorted[K, V] = copy(super.filter(f), valueOrdering)

  // overrides for PairRDDFunctions

  def flatMapValues[W: ClassTag](f: V => TraversableOnce[W]): GroupSorted[K, W] = copy(pair.flatMapValues(f), None)

  def mapValues[W: ClassTag](f: V => W): GroupSorted[K, W] = copy(pair.mapValues(f), None)

  // GroupSorted specific

  def mapKeyValuesToValues[W: ClassTag](f: ((K, V)) => W): GroupSorted[K, W] = copy(super.mapPartitions(_.map(kv => (kv._1, f(kv))), true), None)

  def mapStreamByKey[W: ClassTag](f: Iterator[V] => TraversableOnce[W]): GroupSorted[K, W] = copy(super.mapPartitions(mapStreamIterator(_)(f), true), None)

  def mapStreamByKey[W: ClassTag, C](c: () => C)(f: (C, Iterator[V]) => TraversableOnce[W]): GroupSorted[K, W] = copy(super.mapPartitions(mapStreamIteratorWithContext(_)(c, f), true), None)

  def foldLeftByKey[W: ClassTag](w: W)(f: (W, V) => W): GroupSorted[K, W] = {
    val wCreate = newWCreate(w)
    mapStreamByKey(iter => Iterator(iter.foldLeft(wCreate())(f)))
  }

  def reduceLeftByKey[W >: V: ClassTag](f: (W, V) => W): GroupSorted[K, W] = mapStreamByKey(iter => Iterator(iter.reduceLeft(f)))

  def scanLeftByKey[W: ClassTag](w: W)(f: (W, V) => W): GroupSorted[K, W] = {
    val wCreate = newWCreate(w)
    mapStreamByKey(_.scanLeft(wCreate())(f))
  }

  def mergeJoin[W: ClassTag](other: GroupSorted[K, W], bufferLeft: Boolean = false): GroupSorted[K, (Option[V], Option[W])] = {
    // this is kind of broken because Scala's implicit Orderings don't have proper equals/hashCode
    // it seems to work for primitives and strings but not for tuples
    // could use org.apache.commons.lang.builder.EqualsBuilder.reflectionEquals but that might throw a SecurityException
    //require(keyOrdering == other.keyOrdering, "key ordering must be the same")

    val partitioner1 = defaultPartitioner(this, other)
    val left = this.partitioner match {
      case Some(partitioner1) => this
      case _ => GroupSorted(this, partitioner1, keyOrdering, None)
    }
    val right = other.partitioner match {
      case Some(partitioner1) => other
      case _ => GroupSorted(other, partitioner1, keyOrdering, None)
    }
    val zipped = left.zipPartitions(right, true)(mergeJoinIterators(_, _, keyOrdering, bufferLeft))
    copy(zipped, None)
  }

  def mergeJoinInner[W: ClassTag](other: GroupSorted[K, W], bufferLeft: Boolean = false): GroupSorted[K, (V, W)] = {
    val joined = mergeJoin(other, bufferLeft).mapPartitions({ iter =>
      iter.collect{ case (k, (Some(v), Some(w))) => (k, (v, w)) }
    }, true)
    copy(joined, None)
  }

  def mergeJoinLeftOuter[W: ClassTag](other: GroupSorted[K, W], bufferLeft: Boolean = false): GroupSorted[K, (V, Option[W])] = {
    val joined = mergeJoin(other).mapPartitions({ iter =>
      iter.collect{ case (k, (Some(v), maybeW)) => (k, (v, maybeW)) }
    }, true)
    copy(joined, None)
  }

  def mergeJoinRightOuter[W: ClassTag](other: GroupSorted[K, W], bufferLeft: Boolean = false): GroupSorted[K, (Option[V], W)] = {
    val joined = mergeJoin(other, bufferLeft).mapPartitions({ iter =>
      iter.collect{ case (k, (maybeV, Some(w))) => (k, (maybeV, w)) }
    }, true)
    copy(joined, None)
  }

  def mergeJoin[W: ClassTag, U: ClassTag](other: GroupSorted[K, W], f: (Iterator[V], Iterator[W]) => TraversableOnce[U]): GroupSorted[K, U] = {
    val partitioner1 = defaultPartitioner(this, other)
    val left = this.partitioner match {
      case Some(partitioner1) => this
      case _ => GroupSorted(this, partitioner1, keyOrdering, None)
    }
    val right = other.partitioner match {
      case Some(partitioner1) => other
      case _ => GroupSorted(other, partitioner1, keyOrdering, None)
    }
    val zipped = left.zipPartitions(right, true)(mergeJoinIterators(_, _, f, keyOrdering))
    copy(zipped, None)
  }

  def mergeUnion(other: GroupSorted[K, V]): GroupSorted[K, V] = {
    val keyValueOrdering = Ordering.by{ kv: (K, V) => kv._1 }(keyOrdering)
    copy(this.zipPartitions(other, true)(mergeUnionIterators(_, _, keyValueOrdering)), None)
  }

  ///** Dangerous, do not use unless you know exactly what you are doing */
  //def preservesGroupSorting(f: RDD[(K, V)] => RDD[(K, W)]): GroupSorted[K, W] = copy(f(this), new GroupSorted(f(this) 
}

object GroupSorted {
  private[sorted] def apply[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], partitioner: Partitioner, keyOrdering: Ordering[K], valueOrdering: Option[Ordering[V]]): GroupSorted[K, V] = {
    valueOrdering match {
      case Some(vo) =>
        val shuffled = new ShuffledRDD[(K, V), Unit, Unit](rdd.map{ kv => (kv, ())}, new KeyPartitioner(partitioner))
          .setKeyOrdering(Ordering.Tuple2(keyOrdering, vo))
          .mapPartitions(_.map(kv => (kv._1._1, kv._1._2)), false)
        val p = partitioner
        val shuffledWithPartitioner = new RDD[(K, V)](shuffled) {
          override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = firstParent[(K, V)].compute(split, context)

          override def getPartitions: Array[Partition] = firstParent[(K, V)].partitions
          
          override val partitioner: Option[Partitioner] = Some(p)
        }
        new GroupSorted(shuffledWithPartitioner, keyOrdering, Some(vo))
      case None =>
        val shuffled = new ShuffledRDD[K, V, V](rdd, partitioner)
          .setKeyOrdering(keyOrdering)
        new GroupSorted(shuffled, keyOrdering, None)
    }
  }

  def apply[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], partitioner: Partitioner, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = {
    val keyHashOrdering = new HashOrdering(keyOrdering)
    GroupSorted(rdd, partitioner, keyHashOrdering, valueOrdering)
  }

  def apply[K: ClassTag, V: ClassTag, C: ClassTag](rdd: RDD[(K, V)], partitioner: Partitioner, createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)(
    implicit keyOrdering: Ordering[K]): GroupSorted[K, C] = {
    val keyHashOrdering = new HashOrdering(keyOrdering)
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    val shuffled = new ShuffledRDD[K, V, C](rdd, partitioner)
      .setKeyOrdering(keyHashOrdering)
      .setAggregator(aggregator)
      .setMapSideCombine(true)
    new GroupSorted(shuffled, keyHashOrdering, None)
  }
}
