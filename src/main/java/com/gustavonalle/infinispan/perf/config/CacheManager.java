package com.gustavonalle.infinispan.perf.config;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
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
        Configuration configuration = new ConfigurationBuilder()
                .jmxStatistics().enable()
                .clustering().cacheMode(CacheMode.REPL_SYNC)
                .stateTransfer().
                        timeout(30 * 60 * 1000)
                .chunkSize(2000000)
                .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
                .indexing().index(Index.ALL)
//                .indexing().enable().indexLocalOnly(false)   // JDG
//            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
                .addProperty("default.indexmanager", "near-real-time")
//            .addProperty("default.chunk_size", String.valueOf(chunk))
                .addProperty("default.directory_provider", "filesystem")
//            .addProperty("default.directory_provider","infinispan")
                .addProperty("default.indexBase", name + "/index")
//            .addProperty("default.indexBase", name + "/cacheStore")
//            .addProperty("default.worker.execution", "async")
//            .addProperty("default.max_queue_length", "10000")
//            .addProperty("default.indexwriter.merge_factor", "3")
//            .addProperty("default.index_flush_interval", "40000")
//            .addProperty("default.indexwriter.ram_buffer_size", "256")
                .persistence().addSingleFileStore().location(name + "/cacheStore").preload(true).fetchPersistentState(true)
                .build();

        this.cacheManager = new DefaultCacheManager(gc, configuration);
    }

    public Cache<String, Transaction> getCache() {
        return cacheManager.getCache("qci-cache");
    }


    public void stop() {
        cacheManager.stop();

    }
}
