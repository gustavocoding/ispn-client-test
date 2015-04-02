package com.gustavonalle.infinispan.perf;

import com.gustavonalle.infinispan.perf.config.CacheManager;
import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.domain.Transaction;
import com.gustavonalle.infinispan.perf.utils.Conditions;
import com.gustavonalle.infinispan.perf.utils.IndexSizeCondition;
import com.gustavonalle.infinispan.perf.utils.Query;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.infinispan.Cache;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;

import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.gustavonalle.infinispan.perf.utils.Indexer.index;
import static com.gustavonalle.infinispan.perf.utils.Statistics.percentile;

public class Main {


    public static void main(String[] args) throws Exception {
        final int threads = Config.getIntProperty("threads", 500);
        final String nodeName = Config.getStringProperty("name", "n1");
        final int docsPerThread = Config.getIntProperty("dpt", 10000);
        final int nodes = Config.getIntProperty("nodes", 1);
        final int index = Config.getIntProperty("index", 1);
        final int write = Config.getIntProperty("write", 1);
        final int mass = Config.getIntProperty("mass", 0);
        StopTimer globalTimer = new StopTimer();
        CacheManager cacheManager = new CacheManager();
        final Cache<String, Transaction> cache = cacheManager.getCache();
// JDG
//        cache.put("1", new Transaction(2, ""));
//        cache.remove("1");

//        Search.getSearchManager(cache).getSearchFactory().addClasses(Transaction.class);

//      final boolean isIndexEnabled = cache.getCacheConfiguration().indexing().index().isEnabled();
        // JDG
        final boolean isIndexEnabled = index != 0;
        final boolean isWriteEnabled = write != 0;
        StopTimer timer = new StopTimer();

        if (isWriteEnabled) {
            Collection<Future<?>> futures = index(cache, threads, nodeName, docsPerThread, nodes, isIndexEnabled);
            timer.stop();
            System.out.println("\nIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");

            long[][] results = new long[threads][docsPerThread];
            int i = 0;
            for (Future<?> f : futures) {
                results[i++] = (long[]) f.get();
            }

            System.out.println("\n50%perc: " + TimeUnit.MILLISECONDS.convert(percentile(50, results), TimeUnit.NANOSECONDS));
            System.out.println("90%perc: " + TimeUnit.MILLISECONDS.convert(percentile(90, results), TimeUnit.NANOSECONDS));
            System.out.println("95%perc: " + TimeUnit.MILLISECONDS.convert(percentile(95, results), TimeUnit.NANOSECONDS));
        }
        int count = 0;
//        if (isIndexEnabled) {
            Conditions.assertConditionMet(new IndexSizeCondition(cache, threads * docsPerThread * nodes));
            count = Query.count(cache);
//        }
        globalTimer.stop();

//      System.exit(0);
        System.out.println("Node started with index size = " + count + ", cache size = " + cache.size() + " in " + globalTimer.getElapsedIn(TimeUnit.SECONDS) + "s");

        if (mass == 1) {
            MassIndexer massIndexer = Search.getSearchManager(cache).getMassIndexer();
            massIndexer.start();
            System.out.println("Mass Indexer Finished, index size = " + Query.count(cache));

        }
//        cacheManager.stop();

    }

}
