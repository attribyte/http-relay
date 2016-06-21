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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AsyncClient;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.jetty.JettyClient;
import org.attribyte.essem.metrics.HDRReservoir;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.util.InitUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Supplies messages from an HTTP endpoint.
 */
public abstract class HTTPSupplier implements Supplier {

   /**
    * Creates an uninitialized supplier.
    */
   public HTTPSupplier() {
      httpClient = new JettyClient();
      isInit = new AtomicBoolean(false);
   }

   @Override
   public Message nextMessage() {

      if(!isInit.get()) {
         return Message.stop();
      }

      ByteString nextSavedState = stateHistory.poll();
      if(nextSavedState != null) {
         return Message.state(nextSavedState);
      }

      if(state == State.MESSAGE) {
         Optional<Request> maybeRequest = nextRequest();
         if(!maybeRequest.isPresent()) {
            return Message.pause(nextSleepMillis());
         }

         Request request = maybeRequest.get();
         Object id = request.attributes.get("id");
         if(id == null) {
            id = request.getURI();
         }

         final Timer.Context ctx = messageRequests.time();

         try {
            Response response = httpClient.send(request);
            if(response.statusCode / 200 == 1) {
               state = State.PAUSE;
               ByteString body = response.getBody();
               responseSize.update(body.size());
               return Message.publish(id.toString(), body);
            } else {
               requestErrors.inc();
               logger.error(String.format("Supplier status error (%d)", response.statusCode));
               lostMessage(Message.publish(id.toString(), ByteString.EMPTY));
               return Message.pause(nextSleepMillis());
            }
         } catch(IOException ioe) {
            requestErrors.inc();
            logger.error("Supplier I/O error", ioe);
            lostMessage(Message.publish(id.toString(), ByteString.EMPTY));
            return Message.pause(nextSleepMillis());
         } finally {
            ctx.stop();
         }
      } else {
         state = State.MESSAGE;
         return Message.pause(nextSleepMillis());
      }
   }

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {
      if(isInit.compareAndSet(false, true)) {
         InitUtil httpProps = new InitUtil("http.", props);
         sleepMillis = httpProps.getIntProperty("sleepMillis", 1000);
         httpClient.init("http.", props, logger);
         initState(savedState);
      }
   }

   @Override
   public Optional<ByteString> shutdown() {
      if(isInit.compareAndSet(true, false)) {
         try {
            httpClient.shutdown();
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
         catch(Exception e) {
            logger.error("Problem shutting down HTTP client", e);
         }

         List<ByteString> unreportedState = Lists.newArrayListWithExpectedSize(8);
         int numStates = stateHistory.drainTo(unreportedState);
         return numStates == 0 ? Optional.absent() : Optional.of(unreportedState.get(numStates - 1));
      } else {
         return Optional.absent();
      }
   }

   /**
    * Sets the initial state.
    * @param state The initial state.
    */
   protected abstract void initState(Optional<ByteString> state);

   /**
    * Retrieves the next request or <code>absent</code> if no requests are available.
    * <p>
    *    An id may added as an attribute, 'id' of the request.
    *    Otherwise, the URI will be used as the message id.
    * </p>
    * @return The next request.
    */
   protected abstract Optional<Request> nextRequest();

   /**
    * Gets the next (saved) state based on a completed message.
    * @param completedMessage The completed message.
    * @return The next state, if any.
    */
   protected abstract Optional<ByteString> nextState(final Message completedMessage);

   @Override
   public void lostMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      lostMessages.inc();
   }

   @Override
   public void completedMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      Optional<ByteString> nextState = nextState(message);
      if(nextState.isPresent()) {
         try {
            stateHistory.put(nextState.get());
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
      completedMessages.inc();
   }

   /**
    * Times requests to get published messages.
    */
   private final Timer messageRequests = new Timer();

   /**
    * Counts completed messages.
    */
   public final Counter completedMessages = new Counter();

   /**
    * Counts any lost messages.
    */
   public final Counter lostMessages = new Counter();

   /**
    * Counts request errors.
    */
   public final Counter requestErrors = new Counter();

   /**
    * Records response size.
    */
   private final Histogram responseSize = new Histogram(new HDRReservoir(2, HDRReservoir.REPORT_SNAPSHOT_HISTOGRAM));

   /**
    * The time elapsed between message create and the (async) response from the target.
    */
   private final Timer timeToAcknowledge = new Timer();

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("message-requests", messageRequests);
      builder.put("response-size", responseSize);
      builder.put("request-errors", requestErrors);
      builder.put("completed-messages", completedMessages);
      builder.put("lost-messages", lostMessages);
      builder.put("time-to-acknowledge", timeToAcknowledge);
      return builder.build();
   }

   /**
    * Override to customize sleep millis based on state.
    * @return The sleep time in milliseconds.
    */
   protected int nextSleepMillis() {
      return sleepMillis;
   }

   /**
    * The logger.
    */
   protected Logger logger;

   private final AtomicBoolean isInit;

   /**
    * The HTTP client.
    */
   protected final AsyncClient httpClient;

   private int sleepMillis;
   private BlockingQueue<ByteString> stateHistory = new LinkedBlockingQueue<>();

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
      PAUSE
   }

   private State state = State.MESSAGE;

}