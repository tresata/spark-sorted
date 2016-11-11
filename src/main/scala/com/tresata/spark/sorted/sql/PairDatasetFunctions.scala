package com.tresata.spark.sorted.sql

import org.apache.spark.sql.{ Dataset, Encoder }

object PairDatasetFunctions {
  implicit def datasetToSparkSortedPairDatasetFunctions[K: Encoder, V](dataset: Dataset[(K, V)]) = new PairDatasetFunctions(dataset)
}

class PairDatasetFunctions[K: Encoder, V](dataset: Dataset[(K, V)]) extends Serializable {
  def groupSort: GroupSortedDataset[K, V] = GroupSortedDataset(dataset, None)

  def groupSort(numPartitions: Int): GroupSortedDataset[K, V] = GroupSortedDataset(dataset, Some(numPartitions))
}
