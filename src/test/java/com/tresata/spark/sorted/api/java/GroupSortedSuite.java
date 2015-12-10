package com.tresata.spark.sorted.api.java;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.Comparator;
import scala.Tuple2;
import com.google.common.collect.Sets;
import com.google.common.collect.Lists;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.Assert;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import com.tresata.spark.sorted.SparkSuite$;

public class GroupSortedSuite implements Serializable {
  public static class TimeValue {
    private final int time;
    private final Double value;

    public TimeValue(final int time, final Double value) {
      this.time = time;
      this.value = value;
    }
    
    public int getTime() { return time; }
    public Double getValue() { return value; }
  }

  public static class TimeValueComparator implements Comparator<TimeValue>, Serializable {
    public int compare(TimeValue tv1, TimeValue tv2) {
      return tv1.time - tv2.time;
    } 
  }

  private JavaSparkContext jsc() {
    return SparkSuite$.MODULE$.javaSparkContext();
  }

  private <X, Y> Tuple2<X, Y> tuple2(X x, Y y) {
     return new Tuple2<X, Y>(x, y);
  }

  @SuppressWarnings("unchecked")
  private <X> Set<X> set(X x) {
    return Sets.newHashSet(x);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGroupSortNoValueComparator() {
    List<Tuple2<Integer, Integer>> pairs = Lists.newArrayList(tuple2(1, 2), tuple2(2, 3), tuple2(1, 3), tuple2(3, 1), tuple2(2, 1));
    JavaPairRDD<Integer, Integer> p = jsc().parallelizePairs(pairs);
    GroupSorted<Integer, Integer> gs = new GroupSorted(p, new HashPartitioner(2));
    Assert.assertTrue(ImmutableSet.copyOf(gs.glom().collect()).equals(ImmutableSet.of(
      Lists.newArrayList(tuple2(2, 3),  tuple2(2, 1)),
      Lists.newArrayList(tuple2(1, 2),  tuple2(1, 3), tuple2(3, 1)))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGroupSortValueComparator() {
    List<Tuple2<Integer, Integer>> pairs = Lists.newArrayList(tuple2(1, 2), tuple2(2, 3), tuple2(1, 3), tuple2(3, 1), tuple2(2, 1));
    JavaPairRDD<Integer, Integer> p = jsc().parallelizePairs(pairs);
    GroupSorted<Integer, Integer> gs = new GroupSorted(p, new HashPartitioner(2), Ordering.natural());
    Assert.assertTrue(ImmutableSet.copyOf(gs.glom().collect()).equals(ImmutableSet.of(
      Lists.newArrayList(tuple2(2, 1),  tuple2(2, 3)),
      Lists.newArrayList(tuple2(1, 2),  tuple2(1, 3), tuple2(3, 1)))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGroupSortNoEffect() {
    List<Tuple2<Integer, Integer>> pairs = Lists.newArrayList(tuple2(1, 2), tuple2(2, 3), tuple2(1, 3), tuple2(3, 1), tuple2(2, 1));
    JavaPairRDD<Integer, Integer> p = jsc().parallelizePairs(pairs);
    GroupSorted<Integer, Integer> gs = new GroupSorted(p, new HashPartitioner(2), Ordering.natural());
    GroupSorted<Integer, Integer> gs1 = new GroupSorted(gs, new HashPartitioner(2), Ordering.natural());
    Assert.assertTrue(JavaPairRDD.toRDD(gs) == JavaPairRDD.toRDD(gs1));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMapStreamByKeyNoValueComparator() {
    List<Tuple2<String, Integer>> pairs = Lists.newArrayList(tuple2("a", 1), tuple2("b", 10), tuple2("a", 3), tuple2("b", 1), tuple2("c", 5));
    JavaPairRDD<String, Integer> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, Integer> gs = new GroupSorted(p, new HashPartitioner(2));
    JavaPairRDD<String, Set<Integer>> sets = gs.mapStreamByKey(new Function<Iterator<Integer>, Iterator<Set<Integer>>>() {
      public Iterator<Set<Integer>> call(Iterator<Integer> it) {
          return Iterators.singletonIterator((Set<Integer>) Sets.newHashSet(it));
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(sets.collect()).equals(ImmutableSet.of(
      tuple2("a", ImmutableSet.of(1, 3)),
      tuple2("b", ImmutableSet.of(1, 10)),
      tuple2("c", ImmutableSet.of(5)))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMapStreamByKeyValueComparator() {
    List<Tuple2<String, Integer>> pairs = Lists.newArrayList(tuple2("a", 1), tuple2("b", 10), tuple2("a", 3), tuple2("b", 1), tuple2("c", 5));
    JavaPairRDD<String, Integer> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, Integer> gs = new GroupSorted(p, new HashPartitioner(2), Ordering.natural().reverse());
    JavaPairRDD<String, Integer> max = gs.mapStreamByKey(new Function<Iterator<Integer>, Iterator<Integer>>() {
      public Iterator<Integer> call(Iterator<Integer> it) {
        return Iterators.singletonIterator(it.next());
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(max.collect()).equals(ImmutableSet.of(tuple2("a", 3), tuple2("b", 10), tuple2("c", 5))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFoldLeftNoValueComparator() {
    List<Tuple2<String, String>> pairs = Lists.newArrayList(tuple2("c", "x"), tuple2("a", "b"), tuple2("a", "c"), tuple2("b", "e"), tuple2("b", "d"));
    JavaPairRDD<String, String> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, String> gs = new GroupSorted(p, new HashPartitioner(2));
    JavaPairRDD<String, Set<String>> sets = gs.foldLeftByKey(Sets.<String>newHashSet(), new Function2<Set<String>, String, Set<String>>() {
      public Set<String> call(Set<String> acc, String x) {
        // watch out with just mutating here, it wont work
        // i am not sure what is the optional way do this on java
        // what i am doing here is clearly extremely ineffficient
        Set<String> newAcc = Sets.newHashSet(acc);
        newAcc.add(x);
        return newAcc;
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(sets.collect()).equals(ImmutableSet.of(
      tuple2("a", ImmutableSet.of("b", "c")),
      tuple2("b", ImmutableSet.of("d", "e")),
      tuple2("c", ImmutableSet.of("x")))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFoldLeftValueComparator() {
    List<Tuple2<Integer, TimeValue>> pairs = Lists.newArrayList(
      tuple2(5, new TimeValue(2, 0.5)),
      tuple2(1, new TimeValue(1, 1.2)),
      tuple2(5, new TimeValue(1, 1.0)),
      tuple2(1, new TimeValue(2, 2.0)),
      tuple2(1, new TimeValue(3, 3.0)));
    JavaPairRDD<Integer, TimeValue> p = jsc().parallelizePairs(pairs);
    GroupSorted<Integer, TimeValue> gs = new GroupSorted(p, new HashPartitioner(2), new TimeValueComparator());
    JavaPairRDD<Integer, Double> emas = gs.foldLeftByKey(0.0, new Function2<Double, TimeValue, Double>() {
      public Double call(Double acc, TimeValue tv) {
        return 0.8 * acc + 0.2 * tv.getValue();
      }
    });
    System.out.println(ImmutableSet.copyOf(emas.collect()));
    Assert.assertTrue(ImmutableSet.copyOf(emas.collect()).equals(ImmutableSet.of(
      tuple2(1, 1.0736),
      tuple2(5, 0.26))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReduceLeftNoValueComparator() {
    List<Tuple2<String, Set<String>>> pairs = Lists.newArrayList(tuple2("c", set("x")), tuple2("a", set("b")), tuple2("a", set("c")), tuple2("b", set("e")), tuple2("b", set("d")));
    JavaPairRDD<String, Set<String>> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, Set<String>> gs = new GroupSorted(p, new HashPartitioner(2));
    JavaPairRDD<String, Set<String>> sets = gs.reduceLeftByKey(new Function2<Set<String>, Set<String>, Set<String>>() {
      public Set<String> call(Set<String> x, Set<String> y) {
        // watch out with just mutating here, it wont work
        // i am not sure what is the optional way do this on java
        // what i am doing here is clearly extremely ineffficient
        Set<String> newAcc = Sets.newHashSet(x);
        newAcc.addAll(y);
        return newAcc;
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(sets.collect()).equals(ImmutableSet.of(
      tuple2("a", ImmutableSet.of("b", "c")),
      tuple2("b", ImmutableSet.of("d", "e")),
      tuple2("c", ImmutableSet.of("x")))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReduceLeftValueComparator() {
    List<Tuple2<String, String>> pairs = Lists.newArrayList(tuple2("c", "x"), tuple2("a", "b"), tuple2("a", "c"), tuple2("b", "e"), tuple2("b", "d"));
    JavaPairRDD<String, String> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, String> gs = new GroupSorted(p, new HashPartitioner(2), String.CASE_INSENSITIVE_ORDER);
    JavaPairRDD<String, String> sets = gs.reduceLeftByKey(new Function2<String, String, String>() {
      public String call(String x, String y) {
        return x + y;
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(sets.collect()).equals(ImmutableSet.of(
      tuple2("a", "bc"),
      tuple2("b", "de"),
      tuple2("c", "x"))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testScanLeftValueComparator() {
    List<Tuple2<String, String>> pairs = Lists.newArrayList(tuple2("c", "x"), tuple2("a", "b"), tuple2("a", "c"), tuple2("b", "e"), tuple2("b", "d"));
    JavaPairRDD<String, String> p = jsc().parallelizePairs(pairs);
    GroupSorted<String, String> gs = new GroupSorted(p, new HashPartitioner(2), String.CASE_INSENSITIVE_ORDER);
    JavaPairRDD<String, Set<String>> sets = gs.scanLeftByKey(Sets.<String>newHashSet(), new Function2<Set<String>, String, Set<String>>() {
      public Set<String> call(Set<String> acc, String x) {
        // watch out with just mutating here, it wont work
        // i am not sure what is the optional way do this on java
        // what i am doing here is clearly extremely ineffficient
        Set<String> newAcc = Sets.newHashSet(acc);
        newAcc.add(x);
        return newAcc;
      }
    });
    Assert.assertTrue(ImmutableSet.copyOf(sets.collect()).equals(ImmutableSet.of(
      tuple2("a", ImmutableSet.of()),
      tuple2("a", ImmutableSet.of("b")),
      tuple2("a", ImmutableSet.of("b", "c")),
      tuple2("b", ImmutableSet.of()),
      tuple2("b", ImmutableSet.of("d")),
      tuple2("b", ImmutableSet.of("d", "e")),
      tuple2("c", ImmutableSet.of()),
      tuple2("c", ImmutableSet.of("x")))));
  }
}
