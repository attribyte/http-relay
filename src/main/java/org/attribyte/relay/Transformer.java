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

import org.attribyte.api.Logger;

import java.util.Properties;

/**
 * Transforms a replication message from one format to another.
 */
public interface Transformer {

   /**
    * A transformer that returns the input message.
    */
   public static Transformer NOOP = new Transformer() {

      @Override
      public Message transform(Message message) {
         return message;
      }

      @Override
      public void init(final Supplier supplier,
                       final Properties props,
                       final Logger logger) throws Exception {
      }

      @Override
      public void shutdown() {
      }
   };

   /**
    * Transforms a message from one format to another.
    * @param message The message.
    * @return The transformed message.
    */
   public Message transform(Message message);

   /**
    * Initialize the supplier from properties.
    * @param supplier The supplier feeding this transformer.
    * @param props The properties.
    * @param logger A logger.
    * @throws Exception on initialization error.
    */
   public void init(final Supplier supplier,
                    final Properties props,
                    final Logger logger) throws Exception;

   /**
    * Shutdown the supplier.
    */
   public void shutdown();
}
