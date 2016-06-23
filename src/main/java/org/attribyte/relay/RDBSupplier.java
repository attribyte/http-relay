/*
 * Copyright 2016 Attribyte, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.attribyte.relay;

import com.codahale.metrics.Metric;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.attribyte.api.Logger;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.sql.pool.PasswordSource;
import org.attribyte.sql.pool.contrib.PropertiesPasswordSource;
import org.attribyte.util.InitUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A base class for creating suppliers that require a relational database.
 */
public abstract class RDBSupplier implements Supplier {

   /**
    * Called by implementation to initialize pools after construction.
    * @param props The properties.
    * @param logger The logger.
    * @throws Exception on initialization error.
    */
   protected void initPools(final Properties props, final Logger logger) throws Exception {
      InitUtil dbProps = new InitUtil("rdb.", props);
      String credentialsFilename = Strings.nullToEmpty(dbProps.getProperty("credentialsFile")).trim();
      final PasswordSource passwordSource;
      if(!credentialsFilename.isEmpty()) {
         File credentialsFile = new File(credentialsFilename);
         if(!credentialsFile.exists()) {
            throw new Exception(String.format("The 'rdb.credentialsFile', '%s' does not exist", credentialsFilename));
         }

         if(!credentialsFile.canRead()) {
            throw new Exception(String.format("The 'rdb.credentialsFile', '%s' can't be read", credentialsFilename));
         }

         Properties credentialsProps = new Properties();
         credentialsProps.load(new ByteArrayInputStream(Files.toByteArray(credentialsFile)));
         passwordSource = new PropertiesPasswordSource(credentialsProps);
      } else {
         passwordSource = null;
      }

      List<ConnectionPool.Initializer> initializers = ConnectionPool.Initializer.fromProperties(dbProps.getProperties(), passwordSource, logger);
      if(initializers.size() == 0) {
         throw new Exception("No connection pool found");
      }

      String defaultPoolName = Strings.nullToEmpty(dbProps.getProperty("defaultPoolName")).trim();

      if(defaultPoolName.isEmpty()) {
         ImmutableMap.Builder<String, ConnectionPool> connectionPools = ImmutableMap.builder();
         this.defaultConnectionPool = initializers.get(0).createPool();
         connectionPools.put(this.defaultConnectionPool.getName(), this.defaultConnectionPool);
         for(int i = 1; i < initializers.size(); i++) {
            ConnectionPool curr = initializers.get(i).createPool();
            connectionPools.put(curr.getName(), curr);
         }
         this.connectionPools = connectionPools.build();
      } else {
         ImmutableMap.Builder<String, ConnectionPool> connectionPools = ImmutableMap.builder();
         for(ConnectionPool.Initializer init : initializers) {
            ConnectionPool pool = init.createPool();
            connectionPools.put(pool.getName(), pool);
         }
         this.connectionPools = connectionPools.build();
         this.defaultConnectionPool = this.connectionPools.get(defaultPoolName);
         if(this.defaultConnectionPool == null) {
            throw new Exception(String.format("The default connection pool, '%s', was not configured", defaultPoolName));
         }
      }
   }

   /**
    * Shutdown all pools - to be called by implementations during {@code shutdown}.
    */
   protected void shutdownPools() {
      for(ConnectionPool pool : connectionPools.values()) {
         pool.shutdown();
      }
   }

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      for(ConnectionPool pool : connectionPools.values()) {
         builder.put(pool.getName(), pool.getMetrics());
      }
      return builder.build();
   }

   /**
    * The logger.
    */
   protected Logger logger;

   /**
    * The default connection pool.
    */
   protected ConnectionPool defaultConnectionPool;

   /**
    * Named connection pools.
    */
   protected Map<String, ConnectionPool> connectionPools;
}