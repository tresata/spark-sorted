package com.tresata.spark.sorted

import org.apache.spark.Partitioner

private class HashOrdering[A](ord: Ordering[A]) extends Ordering[A] {
  override def compare(x: A, y: A): Int = {
    val h1 = if (x == null) 0 else x.hashCode()
    val h2 = if (y == null) 0 else y.hashCode()
    if (h1 < h2) -1 else if (h1 > h2) 1 else ord.compare(x, y)
  }
}

private class KeyPartitioner(partitioner: Partitioner) extends Partitioner {
  override def numPartitions: Int = partitioner.numPartitions

  override def getPartition(key: Any): Int = partitioner.getPartition(key.asInstanceOf[Tuple2[Any, Any]]._1)
}
