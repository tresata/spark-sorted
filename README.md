[![Build Status](https://travis-ci.org/tresata/spark-sorted.svg?branch=master)](https://travis-ci.org/tresata/spark-sorted)

# spark-sorted
Spark-sorted is a library that aims to make non-reduce type operations on very large groups in spark possible, including support for processing ordered values.  To do so it relies on Spark's new sort-based shuffle and on never materializing the group for a given key but instead representing it by consecutive rows within a partition that get processed with a map-like (iterator based streaming) operation.

## GroupSorted
GroupSorted is a trait for key-value RDDs that also satisfy the following criteria:
* all rows (key, value pairs) for a given key are consecutive and in the same partition
* the values can optionally be ordered per key

GroupSorted can be created from a RDD[(K, V)] using the rdd.groupSort operator. To enable the groupSort operator add the following import:
```
import com.tresata.spark.sorted.PairRDDFunctions._
```

GroupSorted adds 2 methods to a key-value RDD to process all values records for a given key: mapStreamByKey and foldLeftByKey.

For example say you have a data-set of stock prices, represented as follows:
```
type Ticker = String
case class Quote(time: Int, price: Double)
val prices: RDD[(Ticker, Quote)] = ...
```
Assuming you have a function calculates exponential moving averages (EMAs), you could produce time series of EMAs for all tickers as follows:
```
val emas: Iterator[Double] => Iterator[Double] = ...
prices.groupSorted(Ordering.by[Quote, Int](_.time)).mapStreamByKey{ iter => emas(iter.map(_.price)) }
```

Currently this library is alpha stage.

Have fun!
Team @ Tresata
