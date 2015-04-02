package com.gustavonalle.infinispan.perf;

import com.gustavonalle.infinispan.perf.utils.Condition;
import com.gustavonalle.infinispan.perf.utils.Conditions;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.client.helpers.Operations;
import org.jboss.dmr.ModelNode;

import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.gustavonalle.infinispan.perf.utils.Indexer.index;

public class RemoteFleet {


    public static void main(String[] args) throws Exception {

        int numClient = 1;
        final int numThreads = 500;
        final int docsPerThread = 10000;
        final String host = "localhost";
        final int port = 11222;


        ExternalWildFly server = new ExternalWildFly(
                "/home/gfernandes/code/infinispan/fork/server/integration/build/target/infinispan-server-7.2.0-SNAPSHOT/bin/standalone.sh",
//                "/home/gfernandes/Downloads/infinispan-server-7.1.0.Final/bin/standalone.sh",
                11222,
                9990
        );

        server.start();

        ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch startLatch = new CountDownLatch(1);
        for (int i = 0; i < numClient; i++) {
            final int finalI = i;
            Runnable r = new Runnable() {
                public void run() {
                    try {
                        startLatch.await();
                        new RemoteClient(numThreads, "node" + finalI, docsPerThread, 1, host, port).work();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            executorService.execute(r);
        }

        startLatch.countDown();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        server.stop();

    }


    static class RemoteClient {
        final int threads;
        final String nodeName;
        final int docsPerThread;
        final int index;
        private final RemoteCache<Object, Object> cache;


        public RemoteClient(int threads, String nodeName, int docsPerThread, int index, String serverHost, int serverPort) {
            this.threads = threads;
            this.nodeName = nodeName;
            this.docsPerThread = docsPerThread;
            this.index = index;
            ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
            clientBuilder.addServer().host(serverHost).port(serverPort);
            RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
            cache = remoteCacheManager.getCache();
        }

        public boolean work() throws Exception {
            StopTimer timer = new StopTimer();
            LatencyStats myOpStats = new LatencyStats();
            if (index == 1) {
                index(cache, threads, nodeName, docsPerThread, myOpStats);
                timer.stop();
                System.out.println();
                Histogram intervalHistogram = myOpStats.getIntervalHistogram();
                intervalHistogram.outputPercentileDistribution(System.out, 1000000d);
                System.out.println("50% percentile: " + intervalHistogram.getValueAtPercentile(50) / 1000000d);
                System.out.println("90% percentile: " + intervalHistogram.getValueAtPercentile(90) / 1000000d);
                System.out.println("95% percentile: " + intervalHistogram.getValueAtPercentile(95) / 1000000d);
                System.out.println("99% percentile: " + intervalHistogram.getValueAtPercentile(99) / 1000000d);
                System.out.println("\nIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");
            } else {
                cache.put("root0", "0abcdef");
            }

            System.out.println();
            return true;

        }
    }

    public static class ExternalWildFly {

        private final String startScriptPath;
        private final int portToWaitFor;
        private final int adminPort;

        public ExternalWildFly(String startScriptPath, int portToWaitFor, int adminPort) {
            this.startScriptPath = startScriptPath;
            this.portToWaitFor = portToWaitFor;
            this.adminPort = adminPort;
        }

        public void start() throws Exception {
            new ProcessBuilder(startScriptPath).inheritIO().start();
            Conditions.assertConditionMet(new PortOpenCondition(portToWaitFor));
        }

        public void stop() throws Exception {
            try (final ModelControllerClient client = ModelControllerClient.Factory.create(InetAddress.getLocalHost(), adminPort)) {
                final ModelNode op = Operations.createOperation("shutdown");
                op.get("restart").set(false);
                client.execute(op);
            }
        }
    }


    private static class PortOpenCondition implements Condition {

        private final int port;

        public PortOpenCondition(int port) {
            this.port = port;
        }

        public boolean isOpen() {
            Socket s = null;
            try {
                s = new Socket("localhost", port);
                return true;
            } catch (Exception e) {
                return false;
            } finally {
                if (s != null)
                    try {
                        s.close();
                    } catch (Exception ignored) {
                    }
            }
        }

        @Override
        public boolean evaluate() {
            return isOpen();
        }
    }


}
