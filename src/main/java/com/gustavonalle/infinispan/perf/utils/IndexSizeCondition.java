package com.gustavonalle.infinispan.perf.utils;

import org.infinispan.Cache;

public class IndexSizeCondition implements Condition {
   private final Cache<?, ?> cache;
   private final int expectedCount;

   public IndexSizeCondition(Cache<?, ?> cache, int expectedCount) {
      this.cache = cache;
      this.expectedCount = expectedCount;
   }

   @Override
   public boolean evaluate() {
      try {
         Thread.sleep(2000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      int count = Query.count(cache);
      System.out.printf("\rCurrent index size: %d\n", count);
      return count == expectedCount;
   }
}
