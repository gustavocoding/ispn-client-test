package com.gustavonalle.infinispan.perf.utils;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.LatencyUtils.LatencyStats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.context.Flag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.lucene.util.NamedThreadFactory;

public class DataWriter {

   public static void write(final Cache<String, Transaction> cache, int threads, final String prefix,
                            final int docsPerThread, boolean index, int nodes, LatencyStats latencyStats) throws Exception {
      final Cache<String, Transaction> writeCache = index ? cache : cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      final boolean isEnabled = cache.getCacheConfiguration().indexing().index().isEnabled();

      final AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(threads);
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
//                                     count = Query.count(cache);
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
//         Conditions.assertConditionMet(new IndexSizeCondition(cache, expectedSize));
      }

   }

   public static void writeLucene(Directory directory, final IndexWriter indexWriter, int threads, final String prefix,
                                  final int docsPerThread, int nodes, LatencyStats latencyStats) throws Exception {


      final AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(threads);
      Collection<Future<?>> futures = new ArrayList<>(threads);
      TrackingIndexWriter trackingIndexWriter = new TrackingIndexWriter(indexWriter);
      SearcherManager searcherManager = new SearcherManager(indexWriter, true, null);
      ControlledRealTimeReopenThread<IndexSearcher> reopenThread = new ControlledRealTimeReopenThread<>(trackingIndexWriter, searcherManager, 1, 1);
      for (int i = 0; i < threads; i++) {
         futures.add(
                 executorService.submit(
                         () -> {
                            for (int j = 0; j < docsPerThread; j++) {
                               int id = counter.incrementAndGet();
                               long start = System.nanoTime();

                               Document doc = new Document();
                               doc.add(new TextField("script", "0abcdef" + j, Field.Store.NO));
                               doc.add(new TextField("id", prefix + id, Field.Store.NO));
                               try {
                                  trackingIndexWriter.addDocument(doc);
                               } catch (IOException e) {
                                  e.printStackTrace();
                               }
                               latencyStats.recordLatency(System.nanoTime() - start);
                               if (id != 0 && id % 1000 == 0) {
                                  int count = 0;
                                  IndexSearcher acquire = null;
//                                  try {
//                                     indexWriter.commit();
//                                      acquire = searcherManager.acquire();
//                                      count = acquire.getIndexReader().numDocs();
//                                  System.out.printf("\rInserted %d, index is at %d", id, count);
//                                  } catch (IOException e) {
//                                     e.printStackTrace();
//                                  }
                               }
                            }
                         }
                 )
         );
      }
      Futures.waitForAll(futures);
      executorService.shutdown();

//      if (index) {
//         String indexManager = (String) cache.getCacheConfiguration().indexing().properties().get("default.indexmanager");
//         boolean isIndexShared = (indexManager != null && indexManager.equals("org.infinispan.query.indexmanager.InfinispanIndexManager"));
//         boolean isIndexLocal = cache.getCacheConfiguration().indexing().index().isLocalOnly();
//         int expectedSize = threads * docsPerThread;
//         if (isIndexShared || !isIndexLocal) {
//            expectedSize = expectedSize * nodes;
//         }
////         Conditions.assertConditionMet(new IndexSizeCondition(cache, expectedSize));
//      }

   }

   public static Collection<Future<?>> write(
           final RemoteCache cache,
           int threads,
           final String prefix,
           final int docsPerThread, final LatencyStats myOpStats) throws Exception {
      final AtomicInteger counter = new AtomicInteger(0);
//      ExecutorService executorService = Executors.newFixedThreadPool(threads, new NamedThreadFactory("Index Populator"));
      ExecutorService executorService = Executors.newFixedThreadPool(threads);
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
