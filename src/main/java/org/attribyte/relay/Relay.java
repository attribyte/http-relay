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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Header;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.essem.metrics.HDRReservoir;
import org.attribyte.metrics.Reporting;
import org.attribyte.util.InitUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronously relays messages from a supplier
 * to an HTTP endpoint with configurable authentication
 * and retry for failed messages.
 *
 * <b>Configuration Properties</b>
 *
 * <dl>
 *    <dt>relay.acceptCodes</dt>
 *    <dd>
 *       A comma or space separated list of HTTP codes that indicate
 *       the message was accepted. Default is 202.
 *    </dd>
 *
 *    <dt>relay.concurrency</dt>
 *    <dd>
 *       The maximum number of concurrent requests. Default is 2.
 *    </dd>
 *
 *    <dt>relay.maxQueueSize</dt>
 *    <dd>
 *       The maximum number of requests queued for send. Default is unlimited.
 *    </dd>
 *
 *    <dt>relay.notificationTimeoutSeconds</dt>
 *    <dd>
 *       The maximum amount of time to wait for the target to acknowledge
 *       the notification.
 *    </dd>
 *
 *    <dt>relay.maxRetryAttempts</dt>
 *    <dd>
 *       The maximum number of times to retry a notification before giving up
 *       and marking it "lost". Default 10.
 *    </dd>
 *
 *    <dt>relay.baseBackOffDelayMillis</dt>
 *    <dd>
 *       The starting number of milliseconds for exponential back-off
 *       on retry. Default is 50.
 *    </dd>
 *
 *    <dt>relay.maxShutdownWaitSeconds</dt>
 *    <dd>
 *       The maximum amount of time to wait while operations complete
 *       after shutdown is indicated.
 *    </dd>
 *
 *    <dt>target.notificationURL</dt>
 *    <dd>
 *       The URL to which notifications are posted.
 *    </dd>
 *
 *    <dt>target.username/target.password</dt>
 *    <dd>
 *       The username and password required for Basic authentication, if any.
 *    </dd>
 *
 *    <dt>target.headers</dt>
 *    <dd>
 *       Any headers that should be sent with every request.
 *       Format: header1_name=header1_value, header2_name=header2_value, ...
 *    </dd>
 *
 *    <dt>supplier.class</dt>
 *    <dd>
 *       The name of the class that supplies notifications.
 *    </dd>
 *
 *    <dt>supplier.state.file</dt>
 *    <dd>
 *       A file used to save internal state. If it is present on initialization,
 *       it provides the initial state.
 *    </dd>
 *
 *    <dt>transformer.class</dt>
 *    <dd>
 *       The name of the class that transforms messages from the input
 *       to notification format. Optional.
 *    </dd>
 *
 * </dl>
 */
public class Relay implements MetricSet {

   public static void main(String[] args) throws Exception {

      final Relay relay = new Relay(args);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         relay.shutdown();
      }));

      System.out.println("Starting relay...");
      relay.start();
   }

   /**
    * Creates a relay from command line arguments.
    * <p>
    *    Arguments that begin with '-' are added as configuration properties.
    *    Other arguments are expected to be configuration files.
    *    Files that start with 'log.' are used to configure loggers.
    * </p>
    * @param args The arguments.
    * @throws Exception on initialization error.
    */
   public Relay(String[] args) throws Exception {

      this.registry = new MetricRegistry();

      CLI cli = new CLI("relay", args);
      this.logger = cli.logger != null ? cli.logger : new ConsoleLogger();

      InitUtil relayProps = new InitUtil("relay.", cli.props, false);

      String acceptCodes = relayProps.getProperty("acceptCodes", Integer.toString(Publisher.HTTP_ACCEPTED));
      Set<Integer> acceptCodeSet = Sets.newHashSet();
      for(String codeStr : Splitter.on(", ").omitEmptyStrings().trimResults().split(acceptCodes)) {
         Integer code = Ints.tryParse(codeStr);
         if(code != null) {
            acceptCodeSet.add(code);
         }
      }

      cli.logInfo("Creating async publisher...");

      this.publisher = new AsyncPublisher(
              relayProps.getIntProperty("concurrency", 2),
              relayProps.getIntProperty("maxQueueSize", 0),
              relayProps.getIntProperty("notificationTimeoutSeconds", 30),
              acceptCodeSet
      );

      registry.register("async-publisher", this.publisher);

      InitUtil targetProps = new InitUtil("target.", cli.props, false);

      this.targetURL = targetProps.getProperty("notificationURL", "").trim();
      if(this.targetURL.isEmpty()) {
         throw new Exception("A 'target.notificationURL' must be supplied");
      }

      List<Header> headers = Lists.newArrayListWithExpectedSize(4);

      if(targetProps.hasProperty("username") && targetProps.hasProperty("password")) {
         final String authHeaderValue = BasicAuthScheme.buildAuthHeaderValue(
                 targetProps.getProperty("username", "").trim(),
                 targetProps.getProperty("password", "").trim()
         );
         headers.add(new Header(BasicAuthScheme.AUTH_HEADER, authHeaderValue));
      }

      if(targetProps.hasProperty("headers")) {
         Splitter.on(',')
                 .trimResults()
                 .omitEmptyStrings()
                 .withKeyValueSeparator('=')
                 .split(targetProps.getProperty("headers"))
                 .entrySet()
                 .stream()
                 .forEach(kv -> headers.add(new Header(kv.getKey(), kv.getValue())));
      }

      this.headers = ImmutableList.copyOf(headers);

      InitUtil supplierProps = new InitUtil("supplier.", cli.props, false);

      cli.logInfo("Creating supplier...");
      this.supplier = (Supplier)supplierProps.initClass("class", Supplier.class);

      cli.logInfo("Creating transformer...");
      InitUtil transformerProps = new InitUtil("transformer.", cli.props, false);
      if(transformerProps.getProperty("class", "").trim().isEmpty()) {
         this.transformer = Transformer.NOOP;
      } else {
         this.transformer = (Transformer)transformerProps.initClass("class", Transformer.class);
      }

      cli.logInfo("Initializing supplier...");

      final Optional<ByteString> savedState;
      String supplierStateFile = supplierProps.getProperty("state.file", "").trim();
      if(!supplierStateFile.isEmpty()) {
         cli.logInfo("Initializing saved supplier state...");
         this.supplierStateFile = new File(supplierStateFile);
         final Optional<ByteString> state;
         if(this.supplierStateFile.exists()) {
            state = Optional.of(ByteString.copyFrom(Files.toByteArray(this.supplierStateFile)));
         } else {
            state = Optional.absent();
         }
         savedState = state;
      } else {
         this.supplierStateFile = null;
         savedState = Optional.absent();
      }

      this.supplier.init(supplierProps.getProperties(), savedState, logger);
      registry.register("supplier", this.supplier);

      cli.logInfo("Initializing transformer...");
      this.transformer.init(this.supplier, transformerProps.getProperties(), logger);
      registry.register("transformer", this.transformer);

      int finishExecutorPoolSize = relayProps.getIntProperty("finishExecutorPoolSize", 2);
      this.finishExecutor = Executors.newFixedThreadPool(finishExecutorPoolSize,
              new ThreadFactoryBuilder().setNameFormat("relay-finish-%d").build()
              );

      int retryExecutorPoolSize = relayProps.getIntProperty("retryExecutorPoolSize", 2);
      this.retryExecutor =
              MoreExecutors.listeningDecorator(
              new ScheduledThreadPoolExecutor(retryExecutorPoolSize,
                      new ThreadFactoryBuilder().setNameFormat("relay-retry-%d").build(),
                      new ThreadPoolExecutor.AbortPolicy()
              )
      );

      this.maxRetryAttempts = relayProps.getIntProperty("maxRetryAttempts", 10);
      this.baseBackOffDelayMillis = relayProps.getIntProperty("baseBackOffDelayMillis", 50);
      this.maxShutdownWaitSeconds = relayProps.getIntProperty("maxShutdownWaitSeconds", 30);
      this.reporting = new Reporting("metrics-reporting.", cli.props, registry, null);
      this.reporting.start();
   }

   /**
    * Starts the relay.
    * <p>
    *    Runs continuously until a 'STOP' message is received from
    *    the supplier.
    * </p>
    * @throws InterruptedException if relay is interrupted.
    */
   public void start() throws Exception {
      if(isInit.compareAndSet(false, true)) {
         publisher.start();
         registry.register("relay", this);

         Message originalMessage = supplier.nextMessage();
         Message transformedMessage = transformer.transform(originalMessage);

         while(transformedMessage.type != Message.Type.STOP) {
            switch(transformedMessage.type) {
               case PUBLISH:
                  messageSize.update(transformedMessage.message.size());
                  addCallback(originalMessage, transformedMessage,
                          publisher.enqueueNotification(new Publisher.Notification(targetURL, transformedMessage.message), headers));
                  break;
               case PAUSE:
                  relayPauses.inc();
                  long sleepMillis = transformedMessage.toInt();
                  if(sleepMillis > 0L) {
                     Thread.sleep(sleepMillis);
                  }
                  break;
               case STATE:
                  if(supplierStateFile != null) {
                     savedStates.inc();
                     try {
                        Files.write(transformedMessage.message.toByteArray(), supplierStateFile);
                     } catch(IOException ioe) {
                        saveStateErrors.inc();
                        logger.error("Problem writing supplier state", ioe);
                     }
                  }
                  break;
               case ERROR:
                  relayErrors.inc();
                  logger.error(transformedMessage.toString());
                  break;
            }
            originalMessage = supplier.nextMessage();
            transformedMessage = transformer.transform(originalMessage);
         }

         shutdown();
      }
   }

   /**
    * Stops the relay and shuts down all resources.
    */
   public final void shutdown() {
      if(isInit.compareAndSet(true, false)) {
         logger.info("Stopping metrics reporting...");
         reporting.stop();

         logger.info("Stopping relay...");

         logger.info("Stopping supplier...");
         supplier.shutdown();

         logger.info("Shutting down publisher...");
         try {
            publisher.shutdown(maxShutdownWaitSeconds);
         } catch(Exception e) {
            logger.error("Problem shutting down publisher", e);
         }

         logger.info("Shutting down retry executor...");
         retryExecutor.shutdown();
         try {
            retryExecutor.awaitTermination(maxShutdownWaitSeconds, TimeUnit.SECONDS);
            if(!retryExecutor.isShutdown()) {
               logger.error("Retry executor did not complete. Shutting down now!");
               retryExecutor.shutdownNow();
            }
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.info("Interrupted while shutting down retry executor...");
         }
         finishExecutor.shutdownNow();
         logger.info("Relay stopped!");
      }
   }

   /**
    * Schedules a message retry.
    * @param originalMessage The original message, as received.
    * @param transformedMessage The transformed message.*
    */
   private void scheduleRetry(final Message originalMessage,
                              final Message transformedMessage) {
      if(transformedMessage.attempts < maxRetryAttempts) {
         notificationRetries.inc();
         retryExecutor.schedule(new Runnable() {
            @Override
            public void run() {
               addCallback(originalMessage, transformedMessage,
                       publisher.enqueueNotification(new Publisher.Notification(targetURL, transformedMessage.message), headers));
            }
         }, transformedMessage.nextBackOffTime(baseBackOffDelayMillis), TimeUnit.MILLISECONDS);
      } else {
         supplier.lostMessage(originalMessage);
         abandonedNotifications.inc();
      }
   }

   /**
    * Adds a callback for a notification result.
    * @param originalMessage The original message, as received.
    * @param transformedMessage The transformed message.
    * @param fut The future result.
    */
   private void addCallback(final Message originalMessage,
                            final Message transformedMessage,
                            final ListenableFuture<Publisher.NotificationResult> fut) {

      Futures.addCallback(fut, new FutureCallback<Publisher.NotificationResult>() {
         public void onSuccess(Publisher.NotificationResult result) {
            if (result.code > 199 && result.code < 300) {
               logger.info("Completed " + transformedMessage.id);
               supplier.completedMessage(originalMessage);
            } else {
               logError(result, transformedMessage.id);
               scheduleRetry(originalMessage.incrementAttempts(), transformedMessage.incrementAttempts());
            }
         }
         public void onFailure(Throwable t) {
            logger.error(transformedMessage.id + " failed", t);
            scheduleRetry(originalMessage.incrementAttempts(), transformedMessage.incrementAttempts());
         }
      }, finishExecutor);

   }

   /**
    * Logs an error if a notification result is an error.
    * @param result The result.
    */
   private void logError(final Publisher.NotificationResult result, final String id) {
      if(result.isError) {
         String message = id + " failed with status = " + Integer.toString(result.code);
         if(result.message.isPresent()) {
            message = message + " - " + result.message.get();
         }

         if(result.cause.isPresent()) {
            logger.error(message, result.cause.get());
         } else {
            logger.error(message);
         }
      }
   }

   /**
    * Tracks the size of relayed messages.
    */
   private final Histogram messageSize = new Histogram(new HDRReservoir(2, HDRReservoir.REPORT_SNAPSHOT_HISTOGRAM));

   /**
    * Tracks relay errors.
    */
   private final Counter relayErrors = new Counter();

   /**
    * Tracks relay pauses.
    */
   private final Counter relayPauses = new Counter();

   /**
    * Tracks number of times state is saved.
    */
   private final Counter savedStates = new Counter();

   /**
    * Counts any errors while saving state.
    */
   private final Counter saveStateErrors = new Counter();

   /**
    * Counts retry after notification failure.
    */
   private final Counter notificationRetries = new Counter();

   /**
    * Counts any notifications that reach the maximum
    * number of retries without success.
    */
   private final Counter abandonedNotifications = new Counter();

   /**
    * Gets metrics for registration.
    * @return The metrics.
    */
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("relay-errors", relayErrors);
      builder.put("relay-pauses", relayPauses);
      builder.put("notification-retries", notificationRetries);
      builder.put("abandoned-notifications", abandonedNotifications);
      builder.put("save-state", savedStates);
      builder.put("save-state-errors", saveStateErrors);
      return builder.build();
   }

   /**
    * The URL to which notifications are published.
    */
   private final String targetURL;

   /**
    * Headers added to every request.
    */
   private final ImmutableList<Header> headers;

   /**
    * The supplier.
    */
   private final Supplier supplier;

   /**
    * The transformer.
    */
   private final Transformer transformer;

   /**
    * The async HTTP publisher.
    */
   private final AsyncPublisher publisher;

   /**
    * An executor for processing completed responses.
    */
   private final ExecutorService finishExecutor;

   /**
    * A scheduled executor for notification retry.
    */
   private final ListeningScheduledExecutorService retryExecutor;

   /**
    * The logger.
    */
   private final Logger logger;

   /**
    * The maximum number of times a notification is tried.
    */
   private final int maxRetryAttempts;

   /**
    * The base back-off delay in milliseconds.
    */
   private final int baseBackOffDelayMillis;

   /**
    * The maximum amount of time to wait for completion before shutdown.
    */
   private final int maxShutdownWaitSeconds;

   /**
    * A file used to store supplier state.
    */
   private final File supplierStateFile;

   /**
    * The metrics registry.
    */
   private final MetricRegistry registry;

   /**
    * Metrics reporting.
    */
   private final Reporting reporting;

   /**
    * Is the relay initialized and running?
    */
   private final AtomicBoolean isInit = new AtomicBoolean(false);
}
