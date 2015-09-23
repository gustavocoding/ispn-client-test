package com.gustavonalle.infinispan.perf;
//

import com.gustavonalle.infinispan.perf.config.Config;
import com.gustavonalle.infinispan.perf.domain.Transaction;
import com.gustavonalle.infinispan.perf.utils.DataWriter;
import com.gustavonalle.infinispan.perf.utils.StopTimer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;

import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class MainRemote {

   public static void main(String[] args) throws Exception {
      int threads = Config.getIntProperty("threads", 1);
      final String nodeName = Config.getStringProperty("name", "n1");
      final int docsPerThread = Config.getIntProperty("dpt", 5000);
      final int index = Config.getIntProperty("write", 1);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
              .host("127.0.0.1").port(11222);

      clientBuilder.marshaller(new ProtoStreamMarshaller());

      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());


      //initialize client-side serialization context
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String build = protoSchemaBuilder.fileName("transaction.proto")
              .addClass(Transaction.class)
              .build(serializationContext);


      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("transaction.proto", build);

      RemoteCache<Object, Object> cache = remoteCacheManager.getCache("gustavo");

      StopTimer timer = new StopTimer();
      if (index == 1) {

         Collection<Future<?>> futures = DataWriter.write(cache, threads, nodeName, docsPerThread, null);
         timer.stop();
         System.out.println("\nIndexing finished in " + timer.getElapsedIn(TimeUnit.SECONDS) + " seconds");

         long[][] results = new long[threads][docsPerThread];
         int i = 0;
         for (Future<?> f : futures) {
            results[i++] = (long[]) f.get();
         }

//            System.out.println("\n50%perc: " + TimeUnit.MILLISECONDS.convert(percentile(50, results), TimeUnit.NANOSECONDS));
//            System.out.println("90%perc: " + TimeUnit.MILLISECONDS.convert(percentile(90, results), TimeUnit.NANOSECONDS));
//            System.out.println("95%perc: " + TimeUnit.MILLISECONDS.convert(percentile(95, results), TimeUnit.NANOSECONDS));
      } else {
         cache.put("root0", "0abcdef");
      }

      System.out.println();

      System.exit(0);

   }

}
