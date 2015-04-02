package com.gustavonalle.infinispan.perf;


import com.gustavonalle.infinispan.perf.config.CacheManager;
import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.domain.Transaction;
import com.gustavonalle.infinispan.perf.utils.Query;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.infinispan.Cache;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;

import java.util.concurrent.TimeUnit;

import static com.gustavonalle.infinispan.perf.utils.Indexer.index;

public class MassIndexerTest {
   public static void main(String[] args) throws Exception {

      int threads = Config.getIntProperty("threads", 50);
      final String nodeName = Config.getStringProperty("name", "n1");
      final int docsPerThread = Config.getIntProperty("dpt", 0);
      final int nodes = Config.getIntProperty("nodes", 1);

      final Cache<String, Transaction> cache = new CacheManager().getCache();
      StopTimer timer = new StopTimer();
      index(cache, threads, nodeName, docsPerThread, nodes, true);
      timer.stop();
      System.out.println("\nIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");

      MassIndexer massIndexer = Search.getSearchManager(cache).getMassIndexer();
      timer = new StopTimer();
      massIndexer.start();
      System.out.println(Query.count(cache));
      timer.stop();
      System.out.println("\nMassIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");


   }
}
