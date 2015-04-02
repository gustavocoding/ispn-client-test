package com.gustavonalle.infinispan.perf.config;

public final class Config {

   public static int getIntProperty(String name, int orElse) {
      String propCfg = System.getProperty(name);
      if (propCfg == null) {
         return orElse;
      } else {
         return Integer.valueOf(propCfg);
      }
   }

   public static String getStringProperty(String name, String orElse) {
      String propCfg = System.getProperty(name);
      if (propCfg == null) {
         return orElse;
      } else {
         return propCfg;
      }
   }

}
