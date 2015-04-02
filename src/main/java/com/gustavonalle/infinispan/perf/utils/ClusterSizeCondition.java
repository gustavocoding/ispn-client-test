package com.gustavonalle.infinispan.perf.utils;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Transport;

public class ClusterSizeCondition implements Condition {
   private final Transport transport;
   private final int expectedSize;

   public ClusterSizeCondition(Cache<?, ?> cache, int expectedSize) {
      this.expectedSize = expectedSize;
      transport = cache.getCacheManager().getTransport();
   }

   @Override
   public boolean evaluate() {
      return this.expectedSize == transport.getMembers().size();
   }
}
