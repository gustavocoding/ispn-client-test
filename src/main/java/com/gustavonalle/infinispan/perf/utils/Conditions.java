package com.gustavonalle.infinispan.perf.utils;


public class Conditions {

   private Conditions() {
   }

   public static void assertConditionMet(Condition condition) throws InterruptedException {
      int maxLoops = 10000;
      int loop = 0;
      int sleep = 100;
      while (!condition.evaluate()) {
         Thread.sleep(sleep);
         if (++loop > maxLoops) {
            throw new RuntimeException("Condition not met because of a timeout");
         }
      }
   }

}
