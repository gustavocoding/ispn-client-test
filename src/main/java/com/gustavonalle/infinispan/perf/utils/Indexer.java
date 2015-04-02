package com.gustavonalle.infinispan.perf.utils;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.LatencyUtils.LatencyStats;
import org.apache.lucene.util.NamedThreadFactory;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.context.Flag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class Indexer {

    public static Collection<Future<?>> index(
            final Cache cache,
            int threads,
            final String prefix,
            final int docsPerThread,
            int nodes, boolean index) throws Exception {
        final Cache writeCache = index ? cache : cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
//      final boolean isEnabled = cache.getCacheConfiguration;().indexing().index().isEnabled();
        // JDG
        final boolean isEnabled = cache.getCacheConfiguration().indexing().enabled();
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(
                threads, new NamedThreadFactory(
                        "Index Populator"
                )
        );
        Collection<Future<?>> futures = new ArrayList<>(threads);
        final Random random = new Random();
        for (int i = 0; i < threads; i++) {
            futures.add(
                    executorService.submit(
                            new Callable<long[]>() {
                                @Override
                                public long[] call() throws InterruptedException {
                                    long[] totalTimes = new long[docsPerThread];
                                    for (int j = 0; j < docsPerThread; j++) {
                                        int id = counter.incrementAndGet();
                                        long start = System.nanoTime();
                                        writeCache.put(prefix + id, new Transaction(id, "0abcdef" + j));
                                        long stop = System.nanoTime();
                                        totalTimes[j] += stop - start;
                                        if (id != 0 && id % 1000 == 0) {
                                            int count = 0;
                                            if (isEnabled) {
                                                count = Query.count(cache);
                                            }
                                            System.out.printf("\rInserted %d, index is at %d", id, count);
                                        }
                                    }
                                    return totalTimes;
                                }
                            }
                    )
            );
        }
        Futures.waitForAll(futures);
        executorService.shutdown();

//		if ( cache.getCacheConfiguration().indexing().index().isEnabled() ) {
        if (cache.getCacheConfiguration().indexing().enabled()) {
//         Conditions.assertConditionMet(new IndexSizeCondition(cache, threads * docsPerThread * nodes));
        }

        return futures;
    }

    public static Collection<Future<?>> index(
            final RemoteCache cache,
            int threads,
            final String prefix,
            final int docsPerThread, final LatencyStats myOpStats) throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(threads, new NamedThreadFactory("Index Populator"));
        Collection<Future<?>> futures = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(executorService.submit(
                            new Runnable() {
                                @Override
                                public void run() {
                                    for (int j = 0; j < docsPerThread; j++) {
                                        int id = counter.incrementAndGet();
                                        long start = System.nanoTime();
                                        cache.put(prefix + id, "0abcdef" + j);
                                        myOpStats.recordLatency(System.nanoTime() - start);
                                        if (id != 0 && id % 1000 == 0) {
                                            System.out.printf("\rInserted %d", id);
                                        }
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
