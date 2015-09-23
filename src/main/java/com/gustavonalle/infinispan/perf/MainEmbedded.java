package com.gustavonalle.infinispan.perf;

import com.gustavonalle.infinispan.perf.config.CacheManager;
import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.domain.Transaction;
import com.gustavonalle.infinispan.perf.utils.ClusterSizeCondition;
import com.gustavonalle.infinispan.perf.utils.Conditions;
import com.gustavonalle.infinispan.perf.utils.DataWriter;
import com.gustavonalle.infinispan.perf.utils.Query;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.infinispan.Cache;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;

import java.util.concurrent.TimeUnit;

/**
 * Run a cache manager configured according to {@link CacheManager}, using TCP gossip
 * for discovery. A Gossip router can be launched quickly with:
 * <p>
 * docker run  -ti -p 12001:12001 gustavonalle/jgroups-gossip
 * </p>
 * Examples:
 * <p>
 * Start 2 cache managers that will use 10 threads each indexing 10k documents per thread as a part of a 2 node cluster. Each cache
 * manager will wait for cluster to form, before start writing:<br>
 * java -jar assembly.jar -Dname=node1 -Dnodes=2 -Dindex=1 -Dwrite=1 -Ddpt=10000 -Dgossip=localhost[12001] <br>
 * java -jar assembly.jar -Dname=node2 -Dnodes=2 -Dindex=1 -Dwrite=1 -Ddpt=10000 -Dgossip=localhost[12001]
 * </p>
 * <p>
 * Start 3 cache managers, with only the first one writing 100 documents with 5 threads, waiting for cluster to
 * form:<br>
 * java -jar assembly.jar -Dname=node1 -Dnodes=3 -Dindex=1 -Dwrite=1 -Dthreads=5 -Ddpt=20 -Dgossip=localhost[12001]
 * <br>
 * java -jar assembly.jar -Dname=node2 -Dnodes=3 -Dindex=0 -Dwrite=0 -Dgossip=localhost[12001] <br>
 * java -jar assembly.jar -Dname=node3 -Dnodes=3 -Dindex=0 -Dwrite=0 -Dgossip=localhost[12001]
 * </p>
 * <p>
 * Start 2 cache managers, first one writing 1M entries without indexing. Later moment, the second will join the cluster and run a mass indexer:<br>
 * java -jar assembly.jar -Dname=node1 -Dnodes=1 -Dindex=0 -Dwrite=1 -Dthreads=100 -Ddpt=10000 -Dgossip=localhost[12001] <br>
 * (later) <br>
 * java -jar assembly.jar -Dname=node2 -Dnodes=2 -Dindex=0 -Dwrite=0 -Dmass=1 -Dgossip=localhost[12001]
 * </p>
 */
public class MainEmbedded {

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

      /**
       * Switch to enable/disable indexing
       */
      final int index = Config.getIntProperty("write", 1);

      /**
       * Switch to enable/disable cache put, useful to join a cache manager
       */
      final int write = Config.getIntProperty("write", 1);

      /**
       * Enable/disable mass indexing when starting
       */
      final int mass = Config.getIntProperty("mass", 0);

      StopTimer globalTimer = new StopTimer();
      LatencyStats latencyStats = new LatencyStats();
      CacheManager cacheManager = new CacheManager();
      final Cache<String, Transaction> cache = cacheManager.getCache();

      Conditions.assertConditionMet(new ClusterSizeCondition(cache, nodes));

      final boolean isIndexEnabled = index != 0;
      final boolean isWriteEnabled = write != 0;
      StopTimer timer = new StopTimer();

      if (isWriteEnabled) {
         DataWriter.write(cache, threads, nodeName, docsPerThread, isIndexEnabled, nodes, latencyStats);
         timer.stop();
         System.out.println("\nIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");
         Histogram intervalHistogram = latencyStats.getIntervalHistogram();
         intervalHistogram.outputPercentileDistribution(System.out, 1000000.0);
      }
      int count = Query.count(cache);
      globalTimer.stop();

      System.out.println("Node started with index size = " + count + ", cache size = " + cache.size() + " in " + globalTimer.getElapsedIn(TimeUnit.SECONDS) + "s");

      if (mass == 1) {
         System.out.println("Running Mass Indexer");
         MassIndexer massIndexer = Search.getSearchManager(cache).getMassIndexer();
         massIndexer.start();
         System.out.println("Mass DataWriter Finished, write size = " + Query.count(cache));

      }
   }

}
