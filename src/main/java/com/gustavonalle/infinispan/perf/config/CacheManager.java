package com.gustavonalle.infinispan.perf.config;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.leveldb.configuration.CompressionType;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;

import java.io.IOException;


public class CacheManager {
   private final EmbeddedCacheManager cacheManager;


   public CacheManager() throws IOException {

      String name = Config.getStringProperty("name", "default");

      GlobalConfiguration gc = new GlobalConfigurationBuilder()
              .globalJmxStatistics().enable()
              .transport().defaultTransport()
              .addProperty("configurationFile", "jgroups.xml")
              .build();


      int chunk = 1024 * 1024;

      Configuration indexCacheConfiguration = new ConfigurationBuilder()
              .clustering().cacheMode(CacheMode.REPL_SYNC).expiration().wakeUpInterval(-1).locking().useLockStriping(false)
              .persistence().passivation(false)
              .addSingleFileStore().shared(false).preload(false)
              .fetchPersistentState(true).purgeOnStartup(false)
              .location(name + "/cacheStore").build();


      Configuration configuration = new ConfigurationBuilder()
              .jmxStatistics().enable()
              .clustering().cacheMode(CacheMode.DIST_SYNC)
              .stateTransfer().timeout(30 * 60 * 1000)
//              .chunkSize(2000000)
              .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
              .indexing().index(Index.ALL)

//                .indexing().enable().indexLocalOnly(false)   // JDG
              .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
//              .addProperty("default.indexmanager", "near-real-time")
              .addProperty("default.metadata_cachename", "metadata_cache")
              .addProperty("default.data_cachename", "data_cache")
//              .addProperty("hibernate.search.default.exclusive_index_use", "true")
//              .addProperty("hibernate.search.default.indexwriter.ram_buffer_size", "256")
//              .addProperty("default.chunk_size", String.valueOf(chunk))
//              .addProperty("default.directory_provider", "filesystem")
//              .addProperty("default.directory_provider", "ram")
              .addProperty("default.directory_provider", "infinispan")
//              .addProperty("default.indexBase", name + "/index")
//              .addProperty("default.worker.execution", "async")
//              .addProperty("default.max_queue_length", "10000")
//              .addProperty("default.indexwriter.merge_factor", "3")
//              .addProperty("default.index_flush_interval", "40000")
//              .addProperty("default.indexwriter.ram_buffer_size", "256")
//              .persistence().passivation(true)
//              .addStore(LevelDBStoreConfigurationBuilder.class)
//              .location(name + "/cacheStore")
//              .expiredLocation(name + "/cacheStore/expired")
//              .implementationType(LevelDBStoreConfiguration.ImplementationType.JNI)
//              .blockSize(1 * 1024 * 1024)
//              .compressionType(CompressionType.SNAPPY)
//              .cacheSize(1 * 1024 * 1024)
//              .fetchPersistentState(true)
//              .shared(false).purgeOnStartup(false).preload(false).compatibility().enable()
//              .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
//                      false).eviction().maxEntries(500000).strategy(
//                      EvictionStrategy.LIRS).threadPolicy(EvictionThreadPolicy.PIGGYBACK)
//              .persistence().addSingleFileStore().location(name + "/cacheStore").preload(true).fetchPersistentState(true)
              .build();




      this.cacheManager = new DefaultCacheManager(gc, configuration);
      cacheManager.defineConfiguration("metadata_cache", indexCacheConfiguration);
      cacheManager.defineConfiguration("data_cache",indexCacheConfiguration);

   }

   public static void main(String[] args) {
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager();
      Cache<Object, Object> nonExistingCache = defaultCacheManager.getCache("whatever");
      System.out.println(nonExistingCache.getCacheConfiguration().clustering().cacheMode());
   }

   public Cache<String, Transaction> getCache() {
      return cacheManager.getCache("qci-cache");
   }


   public void stop() {
      cacheManager.stop();

   }
}
