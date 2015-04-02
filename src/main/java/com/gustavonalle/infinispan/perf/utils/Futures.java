package com.gustavonalle.infinispan.perf.utils;

import java.util.Collection;
import java.util.concurrent.Future;

public class Futures {

   public static void waitForAll(Collection<Future<?>> futures) throws Exception {
      for (Future<?> future : futures) {
         future.get();
      }
   }

}
