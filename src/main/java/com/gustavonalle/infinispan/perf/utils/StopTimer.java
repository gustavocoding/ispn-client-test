package com.gustavonalle.infinispan.perf.utils;

import java.util.concurrent.TimeUnit;

public class StopTimer {

   private final long start;
   private long elapsed;

   public StopTimer() {
      start = currentTime();
   }

   private long currentTime() {
      return System.currentTimeMillis();
   }

   public void stop() {
      elapsed = currentTime() - start;
   }

   public long getElapsedIn(TimeUnit unit) {
      return unit.convert(elapsed, TimeUnit.MILLISECONDS);
   }
}
