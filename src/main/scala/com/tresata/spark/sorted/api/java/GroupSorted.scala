package com.tresata.spark.sorted.api.java

import java.util.{ Comparator, Iterator => JIterator }
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import org.apache.spark.{ Partitioner, HashPartitioner }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.{ Function => JFunction, Function2 => JFunction2, FlatMapFunction => JFlatMapFunction }

import com.tresata.spark.sorted.{ GroupSorted => SGroupSorted }

object GroupSorted {
  private case class ComparatorOrdering[T](comparator: Comparator[T]) extends Ordering[T] {
    def compare(x: T, y: T) = comparator.compare(x, y)
  }

  private def comparatorToOrdering[T](comparator: Comparator[T]): Ordering[T] = new ComparatorOrdering(comparator)

  private def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  private implicit def ordering[K]: Ordering[K] = comparatorToOrdering(NaturalComparator.get[K])

  private def groupSort[K, V](javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner, valueComparator: Comparator[V]): SGroupSorted[K, V] = {
    implicit def kClassTag: ClassTag[K] = javaPairRDD.kClassTag
    implicit def vClassTag: ClassTag[V] = javaPairRDD.vClassTag
    val valueOrdering = Option(valueComparator).map(comparatorToOrdering)
    SGroupSorted(javaPairRDD.rdd, partitioner, valueOrdering)
  }
}

class GroupSorted[K, V] private (sGroupSorted: SGroupSorted[K, V]) extends JavaPairRDD[K, V](sGroupSorted)(GroupSorted.fakeClassTag[K], GroupSorted.fakeClassTag[V]) {
  def this(javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner, valueComparator: Comparator[V]) =
    this(GroupSorted.groupSort(javaPairRDD, partitioner, valueComparator))

  def this(javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner) =
    this(GroupSorted.groupSort(javaPairRDD, partitioner, null))

  def this(javaPairRDD: JavaPairRDD[K, V], numPartitions: Int, valueComparator: Comparator[V]) =
    this(javaPairRDD, if (numPartitions > 0) new HashPartitioner(numPartitions) else defaultPartitioner(javaPairRDD.rdd), valueComparator)

  def this(javaPairRDD: JavaPairRDD[K, V], numPartitions: Int) =
    this(javaPairRDD, numPartitions, null)

  def this(javaPairRDD: JavaPairRDD[K, V], valueComparator: Comparator[V]) =
    this(javaPairRDD, -1, valueComparator)

  def this(javaPairRDD: JavaPairRDD[K, V]) = this(javaPairRDD, -1, null)

  import GroupSorted._

  override def flatMapValues[W](f: JFlatMapFunction[V, W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.flatMapValues(v => f.call(v).asScala))
  }

  override def mapValues[W](f: JFunction[V, W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.mapValues(v => f.call(v)))
  }

  def mapKeyValuesToValues[W](f: JFunction[Tuple2[K, V], W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.mapKeyValuesToValues(kv => f.call(kv)))
  }

  def mapStreamByKey[W](f: JFunction[JIterator[V], JIterator[W]]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.mapStreamByKey(it => f.call(it.asJava).asScala))
  }

  def foldLeftByKey[W](w: W, f: JFunction2[W, V, W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.foldLeftByKey(w)((w, v) => f.call(w, v)))
  }

  def reduceLeftByKey[W >: V](f: JFunction2[W, V, W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.reduceLeftByKey(f.call))
  }

  def scanLeftByKey[W](w: W, f: JFunction2[W, V, W]): GroupSorted[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new GroupSorted[K, W](sGroupSorted.scanLeftByKey(w)((w, v) => f.call(w, v)))
  }
}
