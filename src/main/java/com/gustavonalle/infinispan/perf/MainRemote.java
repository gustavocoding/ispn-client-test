package com.gustavonalle.infinispan.perf;
//

import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.gustavonalle.infinispan.perf.utils.Indexer.index;
import static com.gustavonalle.infinispan.perf.utils.Statistics.percentile;

public class MainRemote {

    public static void main(String[] args) throws Exception {
        int threads = Config.getIntProperty("threads", 1);
        final String nodeName = Config.getStringProperty("name", "n1");
        final int docsPerThread = Config.getIntProperty("dpt", 1000000);
        final int index = Config.getIntProperty("index", 1);

        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder.addServer()
                .host("127.0.0.1").port(11222);

        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

        RemoteCache<Object, Object> cache = remoteCacheManager.getCache();

        StopTimer timer = new StopTimer();
        if (index == 1) {

            Collection<Future<?>> futures = index(cache, threads, nodeName, docsPerThread, null);
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
        } else {
            cache.put("root0", "0abcdef");
        }

        System.out.println();

        System.exit(0);

    }

}
