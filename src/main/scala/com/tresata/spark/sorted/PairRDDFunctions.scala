package com.tresata.spark.sorted

import scala.reflect.ClassTag

import org.apache.spark.{ Logging, Partitioner, HashPartitioner }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.{ RDD, ShuffledRDD }

object PairRDDFunctions {
  implicit def rddToSparkSortedPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) = new PairRDDFunctions(rdd)
}

class PairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Logging with Serializable {
  def groupSorted(partitioner: Partitioner, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = 
    (self, valueOrdering) match {
      case (gs: GroupSorted[K, V], vo) if gs.valueOrdering == vo =>
        gs
      case (rdd, None) =>
        log.info("creating GroupSorted without value ordering")
        val shuffled = new ShuffledRDD[K, V, V](self, partitioner)
          .setKeyOrdering(keyOrdering)
        GroupSorted(shuffled, None)
      case (rdd, Some(vo)) =>
        log.info("creating GroupSorted with value ordering")
        val shuffled = new ShuffledRDD[(K, V), Unit, Unit](self.map{ kv => (kv, ())}, new KeyPartitioner(partitioner))
          .setKeyOrdering(Ordering.Tuple2(new HashOrdering(keyOrdering), vo))
          .mapPartitions(_.map(kv => (kv._1._1, kv._1._2)), true)
        GroupSorted(shuffled, Some(vo))
    }

  def groupSorted(partitioner: Partitioner, valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSorted(partitioner, Some(valueOrdering))

  def groupSorted(numPartitions: Int, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSorted(new HashPartitioner(numPartitions), valueOrdering)

  def groupSorted(numPartitions: Int, valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSorted(new HashPartitioner(numPartitions), Some(valueOrdering))

  def groupSorted(valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSorted(defaultPartitioner(self), valueOrdering)

  def groupSorted(valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSorted(defaultPartitioner(self), Some(valueOrdering))
}
