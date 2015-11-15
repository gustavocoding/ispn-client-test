package com.gustavonalle.infinispan.perf;

import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.utils.DataWriter;
import org.LatencyUtils.LatencyStats;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.DefaultCacheManager;

public class MainLuceneOverInfinispan {


   public static void main(String[] args) throws Exception {
      /**
       * Number of threads writing to the cache
       */
      final int threads = Config.getIntProperty("threads", 10);

      /**
       * Node name, useful when running multiple CacheManagers in the same
       * server to separate resources such as folders, key names, etc.
       */
      final String nodeName = Config.getStringProperty("name", "n1");

      /**
       * How many docs a thread will write
       */
      final int docsPerThread = Config.getIntProperty("dpt", 1000);

      /**
       * Number of expected nodes in the cluster
       */
      final int nodes = Config.getIntProperty("nodes", 1);

      DefaultCacheManager cacheManager = new DefaultCacheManager("lucene-over-infinispan.xml");

      Cache<Object, Object> metadataCache = cacheManager.getCache("SKYWARE_SERVICE_LUCENE_METADATA_CACHE");
      Cache<Object, Object> luceneDataCache = cacheManager.getCache("SKYWARE_SERVICE_LUCENE_DATA_CACHE");
      Cache<Object, Object> luceneLockingCache = cacheManager.getCache("SKYWARE_SERVICE_LUCENE_LOCKING_CACHE");

      Directory directory = DirectoryBuilder.newDirectoryInstance(metadataCache, luceneDataCache, luceneLockingCache, "myIndex").create();
      LatencyStats latencyStats = new LatencyStats();

      IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(Version.LATEST, new StandardAnalyzer()));

      indexWriter.commit();

      DataWriter.writeLucene(directory,indexWriter, 500, "node1", 1000, 1, latencyStats);

      indexWriter.commit();

      DirectoryReader open = DirectoryReader.open(directory);
      System.out.println(open.numDocs());


   }


}
