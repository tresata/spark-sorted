[![Build Status](https://api-staging.travis-ci.com/tresata/spark-sorted.svg?branch=master)](https://staging.travis-ci.com/github/tresata/spark-sorted)

# spark-sorted
Spark-sorted is a library that aims to make non-reduce type operations on very large groups in spark possible, including support for processing ordered values.  To do so it relies on Spark's new sort-based shuffle and on never materializing the group for a given key but instead representing it by consecutive rows within a partition that get processed with a map-like (iterator based streaming) operation.

## GroupSorted
GroupSorted is a partitioned key-value RDDs that also satisfy the following criteria:
* all rows (key, value pairs) for a given key are consecutive and in the same partition
* the values can optionally be ordered per key

GroupSorted can be created from a RDD[(K, V)] using the rdd.groupSort operator. To enable the groupSort operator add the following import:
```
import com.tresata.spark.sorted.PairRDDFunctions._
```

GroupSorted adds methods to a key-value RDD to process all values records for a given key: mapStreamByKey, foldLeftByKey, reduceLeftByKey and scanLeftByKey.

For example say you have a data-set of stock prices, represented as follows:
```
type Ticker = String
case class Quote(time: Int, price: Double)
val prices: RDD[(Ticker, Quote)] = ...
```
Assuming you have a function calculates exponential moving averages (EMAs), you could produce time series of EMAs for all tickers as follows:
```
val emas: Iterator[Double] => Iterator[Double] = ...
prices.groupSort(Ordering.by[Quote, Int](_.time)).mapStreamByKey{ iter => emas(iter.map(_.price)) }
```

A Java Api is available in package com.tresata.spark.sorted.api.java. Please see the unit tests for usage examples.

Have fun!
Team @ Tresata

## Update Apr 2016

Starting with release 0.7.0 GroupSorted operations will return another GroupSorted where possible, to retain the information on RDD layout (partitioning, ordering of keys, and ordering of values), so that operations can be chained efficiently.

Also, we are introducing the mergeJoin operation on GroupSorted as an experimental feature. A mergeJoin takes advantage of the sorting of keys within partitions to join using a sort-merge join. This can be much faster than the default join for key-value RDDs. It also allows one side to stream through the join (without any buffering), while the other side is buffered in memory, making it more scalable for certain applications. Please note this limitation: since values for one side are fully buffered in memory per key, mergeJoin is probably unsuitable for many-to-many joins.

## Update Nov 2016

Starting with release 1.0.0 Scala 2.10 is no longer supported.

Release 1.0.0 will also add initial support for the groupSort operation on Spark SQL Dataset. For Dataset the design of groupSort is kept very simple: it only works for a key-value Dataset (```Dataset[(K, V)]```) with the grouping by key and sorting by value per key. To sort by anything other than the natural ordering of the values simply do a transformation on the values (see unit tests for examples) to get back to a natural ordering. Unfortunately for Dataset it is not (yet) possible to push the secondary sort into the shuffle but the good news is that the sort is implemented efficiently using the Dataset.sortWithinPartitions operation.

## Update Jan 2017

Starting with release 1.2.0 Java 7 is no longer supported.

## Update June 2020

Starting with release 2.0.0 this library compiles against Spark 3. Because of this Spark 2 and Scala 2.11 are no longer supported. We are still compiling with Java 8.
