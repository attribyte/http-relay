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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 */

package com.attribyte.relay.wp;

import com.attribyte.client.ClientProtos;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.attribyte.api.Logger;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.relay.Message;
import org.attribyte.relay.RDBSupplier;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WPSupplier extends RDBSupplier {

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {

      if(isInit.compareAndSet(false, true)) {
         initPools(props, logger);
         this.db = new DB(defaultConnectionPool, "", null, null, 0); //TODO...

         if(savedState.isPresent()) {
            startMeta = PostMeta.fromBytes(savedState.get());
         } else {
            startMeta = PostMeta.ZERO;
         }

         this.selectSleepMillis = Integer.parseInt(props.getProperty("selectSleepMillis", "30000"));
         this.maxSelected = Integer.parseInt(props.getProperty("maxSelected", "500"));
      }
   }

   @Override
   public Message nextMessage() {

      if(!isInit.get()) {
         return Message.stop();
      }

      switch(state) {
         case MESSAGE:
            state = State.PAUSE;
            try {
               return buildMessage().or(Message.pause(0));
            } catch(SQLException se) {
               logger.error("Message select error", se);
               return Message.error("Message select error: " + se.getMessage());
            }
         case PAUSE:
            state = State.STATE;
            return Message.pause(selectSleepMillis);
         case STATE:
            state = State.MESSAGE;
            return Message.state(startMeta.toBytes());
         default:
            throw new AssertionError("Invalid state!");
      }
   }

   @Override
   public Optional<ByteString> shutdown() {
      if(isInit.compareAndSet(true, false)) {
         shutdownPools();
      }
      return Optional.absent(); //TODO...
   }


   /**
    * Builds the replication message.
    * @return The message or {@code absent} if no new entries.
    * @throws SQLException on database error.
    */
   protected Optional<Message> buildMessage() throws SQLException {

      List<PostMeta> nextMeta = db.selectModifiedPosts(Optional.of(startMeta), maxSelected);
      if(nextMeta.isEmpty()) {
         return Optional.absent();
      }

      ClientProtos.WireMessage.Replication.Builder replicationMessage = ClientProtos.WireMessage.Replication.newBuilder();
      String messageId = this.startMeta.toBytes().toStringUtf8();
      this.startMeta = nextMeta.get(nextMeta.size() - 1);
      return Optional.of(Message.publish(messageId, replicationMessage.build().toByteString()));
   }

   @Override
   public void lostMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      lostMessages.inc();
   }

   @Override
   public void completedMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      completedMessages.inc();
   }

   /**
    * The sleep time between selects.
    */
   private int selectSleepMillis;

   /**
    * The maximum number of posts selected for one message.
    */
   private int maxSelected;

   /**
    * The WP DB.
    */
   private DB db;

   /**
    * The metadata start (timestamp, id) for the next request.
    */
   private PostMeta startMeta;

   /**
    * Is the supplier initialized?
    */
   private final AtomicBoolean isInit = new AtomicBoolean(false);

   /**
    * Internal state.
    */
   private enum State {

      /**
       * Send a message next.
       */
      MESSAGE,

      /**
       * Pause next.
       */
      PAUSE,

      /**
       * Send state next.
       */
      STATE
   }

   /**
    * The current state.
    */
   private State state = State.MESSAGE;

   /**
    * Time to build published messages.
    */
   private final Timer messageBuilds = new Timer();

   /**
    * Counts completed messages.
    */
   private final Counter completedMessages = new Counter();

   /**
    * Counts any lost messages.
    */
   private final Counter lostMessages = new Counter();

   /**
    * The time elapsed between message create and the (async) response from the target.
    */
   private final Timer timeToAcknowledge = new Timer();


   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.putAll(super.getMetrics());
      builder.put("message-builds", messageBuilds);
      builder.put("completed-messages", completedMessages);
      builder.put("lost-messages", lostMessages);
      builder.put("time-to-acknowledge", timeToAcknowledge);
      return builder.build();
   }
}
