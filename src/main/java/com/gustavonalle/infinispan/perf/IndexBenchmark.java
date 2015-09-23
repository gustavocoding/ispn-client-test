package com.gustavonalle.infinispan.perf;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.NamedThreadFactory;
import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.leveldb.configuration.CompressionType;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.transaction.TransactionMode;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone write benchmark
 *
 * @author gustavonalle
 */
public class IndexBenchmark {

   static int NUM_THREADS = 10;
   static int NUM_ELEMENTS_PER_THREAD = 500000;
   static String fileLocation = "/tmp/test/";

   @Indexed
   static class Element implements Serializable {

      public Element(String attributeValue) {
         this.attributeValue = attributeValue;
      }

      @Field(index = Index.YES, analyze = Analyze.NO, store = Store.YES)
      private String attributeValue;
   }

   public static int countIndex(Cache<?, ?> cache) {
      IndexReader indexReader = Search.getSearchManager(cache).getSearchFactory().getIndexReaderAccessor().open(Element.class);
      return indexReader.numDocs();
   }

   public static void main(String[] args) throws InterruptedException {

      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().transport().defaultTransport().clusterName("test").build();

      Configuration configuration = new ConfigurationBuilder()
              .clustering()
              .cacheMode(CacheMode.DIST_SYNC)
              .hash().numOwners(1)
              .indexing().index(org.infinispan.configuration.cache.Index.LOCAL)
              .addProperty("hibernate.search.default.indexBase", fileLocation + "/lucene/")
              .addProperty("hibernate.search.default.indexmanager", "near-real-time")
              .addProperty("hibernate.search.default.indexwriter.ram_buffer_size", "256")
              .addProperty("lucene_version", "LUCENE_CURRENT").
                      transaction().transactionMode(
                      TransactionMode.NON_TRANSACTIONAL)
              .persistence().passivation(true)
              .addStore(LevelDBStoreConfigurationBuilder.class)
              .location(fileLocation + "/leveldb/data/")
              .expiredLocation(fileLocation + "/leveldb/expire")
              .implementationType(LevelDBStoreConfiguration.ImplementationType.JNI)
              .blockSize(1024 * 1024 * 1024)
              .compressionType(CompressionType.SNAPPY)
              .cacheSize(1024 * 1024 * 1024)
              .fetchPersistentState(true)
              .shared(false).purgeOnStartup(false).preload(false).compatibility().enable()//.marshaller(new TupleMarshaller())
              .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
                      false).eviction().maxEntries(50000).strategy(
                      EvictionStrategy.LIRS).threadPolicy(EvictionThreadPolicy.PIGGYBACK)
              .build();

      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(globalConfiguration, configuration);

      Cache<Integer, Element> cache = defaultCacheManager.getCache();

      Search.getSearchManager(cache).getSearchFactory().addClasses(Element.class);
      IndexReader indexReader = Search.getSearchManager(cache).getSearchFactory().getIndexReaderAccessor().open(Element.class);

      final AtomicInteger counter = new AtomicInteger(0);

      ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS, new NamedThreadFactory("Index Populator"));

      long start = System.currentTimeMillis();

      for (int i = 0; i < NUM_THREADS; i++) {
         executorService.submit(new Runnable() {
            @Override
            public void run() {
               for (int j = 0; j < NUM_ELEMENTS_PER_THREAD; j++) {
                  int key = counter.incrementAndGet();
                  String value = key + "-value";
                  cache.put(key, new Element(value));
                  if (key != 0 && key % 10000 == 0) {
                     System.out.printf("\rInserted %d, write is at %d", key, countIndex(cache));
                  }
               }
            }
         });
      }

      int total = NUM_ELEMENTS_PER_THREAD * NUM_THREADS;
      executorService.shutdown();

      while (countIndex(cache) != total) {
         Thread.sleep(100);
      }

      long stop = System.currentTimeMillis();

      System.out.println("\nIndexing finished, Ops per sec: = " + total / ((stop - start) / 1000));

      defaultCacheManager.stop();
   }
}
