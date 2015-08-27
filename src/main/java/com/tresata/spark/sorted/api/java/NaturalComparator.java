package com.tresata.spark.sorted.api.java;

import java.util.Comparator;
import java.io.Serializable;

class NaturalComparator implements Comparator<Comparable>, Serializable {
  private static final NaturalComparator INSTANCE = new NaturalComparator();
  
  @SuppressWarnings("unchecked")
  static <T> Comparator<T> get() { return (Comparator<T>) INSTANCE; }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(final Comparable left, final Comparable right) {
    if (left == null) throw new NullPointerException();
    if (right == null) throw new NullPointerException();
    return left.compareTo(right);
  }
}
