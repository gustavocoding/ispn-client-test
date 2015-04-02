package com.gustavonalle.infinispan.perf.utils;

import java.util.Arrays;
import java.util.List;

public class Statistics {

   public static long[] concatAll(long[]... rest) {
      int totalLength = 0;
      for (long[] array : rest) {
         totalLength += array.length;
      }
      long[] result = Arrays.copyOf(rest[0], totalLength);
      int offset = rest[0].length;
      for (int i = 1; i < rest.length; i++) {
         System.arraycopy(rest[i], 0, result, offset, rest[i].length);
         offset += rest[i].length;

      }
      return result;
   }


   public static Long percentile(double percentile, long[]... data) {
      long[] values = concatAll(data);
      Arrays.sort(values);
      double rank = (percentile / 100) * values.length;
      if (rank < 1) return values[0];
      if (rank >= values.length) return values[values.length - 1];

      return values[(int) Math.floor(rank)];
   }


   public static void main(String[] args) {
      System.out.println(percentile(40, new long[]{15l, 20l, 35l, 40l, 50l}));
   }
}
