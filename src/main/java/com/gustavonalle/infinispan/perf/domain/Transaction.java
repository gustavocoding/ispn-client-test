package com.gustavonalle.infinispan.perf.domain;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
//import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

import java.io.Serializable;

@Indexed
@ProtoMessage(name = "Transaction")
public class Transaction implements Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @ProtoField(number = 1, name = "size", required = true)
   int size;

   @Field
//   @SortableField
   String script;

   public Transaction(int size, String script) {
      this.size = size;
      this.script = script;
   }


   public Transaction() {
   }

   @Override
   public String toString() {
      return "Transaction{" +
              "size=" + size +
              ", script='" + script + '\'' +
              '}';
   }
}
