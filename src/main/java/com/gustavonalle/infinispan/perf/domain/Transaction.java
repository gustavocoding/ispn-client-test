package com.gustavonalle.infinispan.perf.domain;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

import java.io.Serializable;

@Indexed
public class Transaction implements Serializable {

   @Field(analyze = Analyze.NO)
   int size;

   @Field
   String script;

   public Transaction(int size, String script) {
      this.size = size;
      this.script = script;
   }

   @Override
   public String toString() {
      return "Transaction{" +
            "size=" + size +
            ", script='" + script + '\'' +
            '}';
   }
}