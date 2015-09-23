package com.gustavonalle.infinispan.perf.utils;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.LatencyUtils.LatencyStats;
import org.apache.lucene.util.NamedThreadFactory;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.context.Flag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DataWriter {

   public static void write(final Cache<String, Transaction> cache, int threads, final String prefix,
                            final int docsPerThread, boolean index, int nodes, LatencyStats latencyStats) throws Exception {
      final Cache<String, Transaction> writeCache = index ? cache : cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      final boolean isEnabled = cache.getCacheConfiguration().indexing().index().isEnabled();

      final AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(
              threads, new NamedThreadFactory(
                      "Index Populator"
              )
      );
      Collection<Future<?>> futures = new ArrayList<>(threads);
      for (int i = 0; i < threads; i++) {
         futures.add(
                 executorService.submit(
                         () -> {
                            for (int j = 0; j < docsPerThread; j++) {
                               int id = counter.incrementAndGet();
                               long start = System.nanoTime();
                               writeCache.put(prefix + id, new Transaction(id, "0abcdef" + j));
                               latencyStats.recordLatency(System.nanoTime() - start);
                               if (id != 0 && id % 1000 == 0) {
                                  int count = 0;
                                  if (isEnabled) {
                                     count = Query.count(cache);
                                  }
                                  System.out.printf("\rInserted %d, index is at %d", id, count);
                               }
                            }
                         }
                 )
         );
      }
      Futures.waitForAll(futures);
      executorService.shutdown();

      if (index) {
         String indexManager = (String) cache.getCacheConfiguration().indexing().properties().get("default.indexmanager");
         boolean isIndexShared = (indexManager != null && indexManager.equals("org.infinispan.query.indexmanager.InfinispanIndexManager"));
         boolean isIndexLocal = cache.getCacheConfiguration().indexing().index().isLocalOnly();
         int expectedSize = threads * docsPerThread;
         if (isIndexShared || !isIndexLocal) {
            expectedSize = expectedSize * nodes;
         }
         Conditions.assertConditionMet(new IndexSizeCondition(cache, expectedSize));
      }

   }

   public static Collection<Future<?>> write(
           final RemoteCache cache,
           int threads,
           final String prefix,
           final int docsPerThread, final LatencyStats myOpStats) throws Exception {
      final AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(threads, new NamedThreadFactory("Index Populator"));
      Collection<Future<?>> futures = new ArrayList<>(threads);
      for (int i = 0; i < threads; i++) {
         futures.add(executorService.submit(
                         () -> {
                            for (int j = 0; j < docsPerThread; j++) {
                               int id = counter.incrementAndGet();
                               long start = System.nanoTime();
                               cache.put(prefix + id, new Transaction(id, "0abcdef" + j));
                               myOpStats.recordLatency(System.nanoTime() - start);
                               if (id != 0 && id % 1000 == 0) {
                                  System.out.printf("\rInserted %d", id);
                               }
                            }
                         }
                 )
         );
      }
      Futures.waitForAll(futures);
      executorService.shutdown();

      return futures;
   }
}
