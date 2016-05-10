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
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.Logger;

import java.util.Map;
import java.util.Properties;

/**
 * A transformer that does nothing but return the input message.
 */
public class NOOPTransformer implements Transformer {

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

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.of();
   }

}
