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
}
