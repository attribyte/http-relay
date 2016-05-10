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

import com.codahale.metrics.MetricSet;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import org.attribyte.api.Logger;

import java.util.Properties;

/**
 * Supplies messages for relay.
 * <p>
 *    A relay loops continuously, sequentially calling <code>nextMessage</code>
 *    to obtain messages for publication or control messages.
 *    Control messages cause the relay to pause, record state, record an error,
 *    or stop. Published messages are <em>asynchronously</em> reported back
 *    to the supplier with either <code>completedMessage</code> or
 *    <code>lostMessage</code>.
 * </p>
 */
public interface Supplier extends MetricSet {

   /**
    * Gets the next message.
    * @return The next message.
    */
   public Message nextMessage();

   /**
    * Notifies the supplier when a message is completed.
    * @param message The completed message.
    */
   public void completedMessage(Message message);

   /**
    * Notifies the supplier when a message is lost.
    * @param message The lost message.
    */
   public void lostMessage(Message message);

   /**
    * Initialize the supplier from properties.
    * @param configProps Configuration properties.
    * @param savedState Previously saved state, if any.
    * @param logger A logger.
    * @throws Exception on initialization error.
    */
   public void init(final Properties configProps,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception;

   /**
    * Shutdown the supplier.
    * @return The state to be saved, if any.
    */
   public Optional<ByteString> shutdown();
}