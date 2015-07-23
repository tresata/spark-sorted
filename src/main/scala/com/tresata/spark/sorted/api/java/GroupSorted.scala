package com.tresata.spark.sorted.api.java

import java.util.{ Iterator => JIterator, Comparator }
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import com.google.common.base.Optional
import com.google.common.collect.{ Ordering => GuavaOrdering }

import org.apache.spark.{ Partitioner, HashPartitioner }
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.{ Function => JFunction, Function2 => JFunction2 }

import com.tresata.spark.sorted.PairRDDFunctions._
import com.tresata.spark.sorted.{ GroupSorted => SGroupSorted }

object GroupSorted {
  private case class ComparatorOrdering[T](comparator: Comparator[T]) extends Ordering[T] {
    def compare(x: T, y: T) = comparator.compare(x, y)
  }

  private def comparatorToOrdering[T](comparator: Comparator[T]): Ordering[T] = new ComparatorOrdering(comparator)

  private def optionalToOption[T](optional: Optional[T]): Option[T] = if (optional.isPresent) Some(optional.get) else None

  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  private implicit def ordering[K]: Ordering[K] = comparatorToOrdering(GuavaOrdering.natural.asInstanceOf[Comparator[K]])

  private def groupSort[K, V](javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner, valueComparator: Optional[Comparator[V]]): SGroupSorted[K, V] = {
    implicit def kClassTag: ClassTag[K] = javaPairRDD.kClassTag
    implicit def vClassTag: ClassTag[V] = javaPairRDD.vClassTag
    javaPairRDD.rdd.groupSort(partitioner, optionalToOption(valueComparator).map(comparatorToOrdering(_)))
  }
}

class GroupSorted[K, V](javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner, valueComparator: Optional[Comparator[V]])
    extends JavaPairRDD[K, V](GroupSorted.groupSort(javaPairRDD, partitioner, valueComparator))(javaPairRDD.kClassTag, javaPairRDD.vClassTag) {
  def this(javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner, valueComparator: Comparator[V]) =
    this(javaPairRDD, partitioner, Optional.of(valueComparator))

  def this(javaPairRDD: JavaPairRDD[K, V], partitioner: Partitioner) =
    this(javaPairRDD, partitioner, Optional.absent[Comparator[V]])

  def this(javaPairRDD: JavaPairRDD[K, V], numPartitions: Int, valueComparator: Comparator[V]) =
    this(javaPairRDD, new HashPartitioner(numPartitions), Optional.of(valueComparator))

  def this(javaPairRDD: JavaPairRDD[K, V], numPartitions: Int) =
    this(javaPairRDD, new HashPartitioner(numPartitions), Optional.absent[Comparator[V]])

  def this(javaPairRDD: JavaPairRDD[K, V], valueComparator: Comparator[V]) =
    this(javaPairRDD, defaultPartitioner(javaPairRDD.rdd), Optional.of(valueComparator))

  def this(javaPairRDD: JavaPairRDD[K, V]) = 
    this(javaPairRDD, defaultPartitioner(javaPairRDD.rdd), Optional.absent[Comparator[V]])

  import GroupSorted._

  private def sGroupSorted: SGroupSorted[K, V] = rdd.asInstanceOf[SGroupSorted[K, V]]

  def mapStreamByKey[W](f: JFunction[JIterator[V], JIterator[W]]): JavaPairRDD[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new JavaPairRDD[K, W](sGroupSorted.mapStreamByKey(it => f.call(it.asJava).asScala))
  }

  def foldLeftByKey[W](w: W, f: JFunction2[W, V, W]): JavaPairRDD[K, W] = {
    implicit def wClassTag: ClassTag[W] = fakeClassTag[W]
    new JavaPairRDD[K, W](sGroupSorted.foldLeftByKey(w)((w, v) => f.call(w, v)))
  }
}
