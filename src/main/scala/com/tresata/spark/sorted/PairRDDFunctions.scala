package com.tresata.spark.sorted

import scala.reflect.ClassTag

import org.apache.spark.{ Logging, Partitioner, HashPartitioner }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.{ RDD, ShuffledRDD }

object PairRDDFunctions {
  implicit def rddToSparkSortedPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) = new PairRDDFunctions(rdd)
}

class PairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Logging with Serializable {
  def groupSort(partitioner: Partitioner, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] = 
    (self, valueOrdering) match {
      case (gs: GroupSorted[K, V], vo) if gs.valueOrdering == vo && (gs.partitioner match {
        case Some(p) if p == partitioner => true
        case _ => false
      }) =>
        log.info("re-using existing GroupSorted")
        gs
      case (rdd, None) =>
        log.info("creating GroupSorted without value ordering")
        val shuffled = new ShuffledRDD[K, V, V](self, partitioner)
          .setKeyOrdering(new HashOrdering(keyOrdering))
        GroupSorted(shuffled, None)
      case (rdd, Some(vo)) =>
        log.info("creating GroupSorted with value ordering")
        val shuffled = new ShuffledRDD[(K, V), Unit, Unit](self.map{ kv => (kv, ())}, new KeyPartitioner(partitioner))
          .setKeyOrdering(Ordering.Tuple2(new HashOrdering(keyOrdering), vo))
          .mapPartitions(_.map(kv => (kv._1._1, kv._1._2)), true) // the preservesPartitioning=true is a lie, but its fixed in the next line
        GroupSorted(shuffled, Some(vo), Some(partitioner))        // partitioner is fixed here
    }

  def groupSort(partitioner: Partitioner, valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSort(partitioner, Some(valueOrdering))

  def groupSort(numPartitions: Int, valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSort(new HashPartitioner(numPartitions), valueOrdering)

  def groupSort(numPartitions: Int, valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSort(new HashPartitioner(numPartitions), Some(valueOrdering))

  def groupSort(valueOrdering: Option[Ordering[V]])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSort(defaultPartitioner(self), valueOrdering)

  def groupSort(valueOrdering: Ordering[V])(implicit keyOrdering: Ordering[K]): GroupSorted[K, V] =
    groupSort(defaultPartitioner(self), Some(valueOrdering))
}
