package com.tresata.spark.sorted.sql

import scala.reflect.ClassTag

import org.apache.spark.sql.{ Dataset, Encoder }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.encoders.{ encoderFor, ExpressionEncoder }

import com.tresata.spark.sorted.{ mapStreamIterator, mapStreamIteratorWithContext, newWCreate }

object GroupSortedDataset {
  private[sql] def apply[K: Encoder, V](dataset: Dataset[(K, V)], numPartitions: Option[Int], reverse: Boolean): GroupSortedDataset[K, V] = {
    val key = col(dataset.columns.head)
    val value = if (reverse) col(dataset.columns.last).desc else col(dataset.columns.last).asc
    new GroupSortedDataset(numPartitions.map(dataset.repartition(_, key)).getOrElse(dataset.repartition(key)).sortWithinPartitions(key, value))
  }
}

class GroupSortedDataset[K: Encoder, V] private (dataset: Dataset[(K, V)]) extends Serializable {
  def toDS: Dataset[(K, V)] = dataset

  def mapStreamByKey[W: Encoder, C](c: () => C)(f: (C, Iterator[V]) => TraversableOnce[W]): Dataset[(K, W)] = {
    implicit val kwEncoder: Encoder[(K, W)] = ExpressionEncoder.tuple(encoderFor[K], encoderFor[W])
    dataset.mapPartitions(mapStreamIteratorWithContext(_)(c, f))
  }

  def mapStreamByKey[W: Encoder](f: Iterator[V] => TraversableOnce[W]): Dataset[(K, W)] = {
    implicit val kwEncoder: Encoder[(K, W)] = ExpressionEncoder.tuple(encoderFor[K], encoderFor[W])
    dataset.mapPartitions(mapStreamIterator(_)(f))
  }

  def foldLeftByKey[W: ClassTag: Encoder](w: W)(f: (W, V) => W): Dataset[(K, W)] = {
    val wCreate = newWCreate(w)
    mapStreamByKey(iter => Iterator(iter.foldLeft(wCreate())(f)))
  }

  def reduceLeftByKey[W >: V: Encoder](f: (W, V) => W): Dataset[(K, W)] =
    mapStreamByKey(iter => Iterator(iter.reduceLeft(f)))

  def scanLeftByKey[W: ClassTag: Encoder](w: W)(f: (W, V) => W): Dataset[(K, W)] = {
    val wCreate = newWCreate(w)
    mapStreamByKey(_.scanLeft(wCreate())(f))
  }
}
