package com.tresata.spark.sorted

import scala.reflect.ClassTag

import org.apache.spark.{ Partitioner, HashPartitioner }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD

object PairRDDFunctions {
  implicit def rddToSparkSortedPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) = new PairRDDFunctions(rdd)
}

class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
  def groupSort(partitioner: Partitioner, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = GroupSorted(rdd, partitioner, valueOrdering)

  def groupSort(partitioner: Partitioner)(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(partitioner, None)

  def groupSort(numPartitions: Int, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = {
    val partitioner = if (numPartitions > 0) new HashPartitioner(numPartitions) else defaultPartitioner(rdd)
    groupSort(partitioner, valueOrdering)
  }

  def groupSort(numPartitions: Int, valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(numPartitions, Some(valueOrdering))

  def groupSort(numPartitions: Int)(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(numPartitions, None)

  def groupSort(valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(-1, Some(valueOrdering))

  def groupSort()(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(-1, None)

  def groupSort(other: GroupSorted[K, _]): GroupSorted[K, V] = GroupSorted(rdd, other.partitioner.get, other.keyOrdering, None)

  def groupSort[C: ClassTag](partitioner: Partitioner, createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)(implicit keyOrdering: Ordering[K]): GroupSorted[K, C] =
    GroupSorted(rdd, partitioner, createCombiner, mergeValue, mergeCombiners)

  def groupSort[C: ClassTag](numPartitions: Int, createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)(implicit keyOrdering: Ordering[K]): GroupSorted[K, C] = {
    val partitioner = if (numPartitions > 0) new HashPartitioner(numPartitions) else defaultPartitioner(rdd)
    groupSort(partitioner, createCombiner, mergeValue, mergeCombiners)
  }

  def groupSort[C: ClassTag](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)(implicit keyOrdering: Ordering[K]): GroupSorted[K, C] =
    groupSort(-1, createCombiner, mergeValue, mergeCombiners)

  def groupSort(partitioner: Partitioner, plus: (V, V) => V)(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(partitioner, identity[V] _, plus, plus)

  def groupSort(numPartitions: Int, plus: (V, V) => V)(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(numPartitions, identity[V] _, plus, plus)

  def groupSort(plus: (V, V) => V)(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = groupSort(identity[V] _, plus, plus)
}
