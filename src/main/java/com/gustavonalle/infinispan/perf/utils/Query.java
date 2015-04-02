package com.gustavonalle.infinispan.perf.utils;

import com.gustavonalle.infinispan.perf.domain.Transaction;
import org.apache.lucene.index.IndexReader;
import org.infinispan.Cache;
import org.infinispan.query.Search;

public final class Query {

    public static int count(Cache<?, ?> cache) {
        IndexReader indexReader = Search.getSearchManager(cache).getSearchFactory().getIndexReaderAccessor().open(Transaction.class);
        return indexReader.numDocs();
    }

}
