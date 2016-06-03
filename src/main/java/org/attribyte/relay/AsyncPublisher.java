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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Header;
import org.attribyte.essem.metrics.Timer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronously publishes notifications.
 */
public class AsyncPublisher implements Publisher {

   /**
    * Creates a publisher with an unbounded notification queue and
    * 30s notification timeout.
    * @param numProcessors The number of threads processing the notification queue.
    * @param acceptCodes The set of HTTP response codes that map to 'Accept'.
    * @throws Exception on initialization error.
    */
   public AsyncPublisher(final int numProcessors,
                         final Set<Integer> acceptCodes) throws Exception {
      this(numProcessors, 0, 30, acceptCodes, Optional.absent());
   }

   /**
    * Creates a publisher with a specified notification queue size.
    * @param numProcessors The number of threads processing the notification queue.
    * @param maxQueueSize The maximum queue size. If &lt; 1, notification queue is unbounded.
    * @param notificationTimeoutSeconds The notification send timeout.
    * @param acceptCodes The set of HTTP response codes that map to 'Accept'.
    * @throws Exception on initialization error.
    */
   public AsyncPublisher(final int numProcessors, final int maxQueueSize,
                         final int notificationTimeoutSeconds,
                         final Set<Integer> acceptCodes) throws Exception {
      this(numProcessors, maxQueueSize, notificationTimeoutSeconds, acceptCodes, Optional.absent());
   }

   /**
    * Creates a publisher with a specified notification queue size.
    * @param numProcessors The number of threads processing the notification queue.
    * @param maxQueueSize The maximum queue size. If &lt; 1, notification queue is unbounded.
    * @param notificationTimeoutSeconds The notification send timeout.
    * @param acceptCodes The set of HTTP response codes that map to 'Accept'.
    * @param logger An optional logger.
    * @throws Exception on initialization error.
    */
   public AsyncPublisher(final int numProcessors, final int maxQueueSize,
                         final int notificationTimeoutSeconds,
                         final Set<Integer> acceptCodes,
                         final Optional<Logger> logger) throws Exception {
      final BlockingQueue<Runnable> notifications;
      assert (numProcessors > 0);
      if(maxQueueSize > 0) {
         notifications = new ArrayBlockingQueue<>(maxQueueSize);
      } else {
         notifications = new LinkedBlockingQueue<>();
      }


      ThreadPoolExecutor executor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS,
              notifications, new ThreadFactoryBuilder().setNameFormat("async-publisher-%d").build(), new ThreadPoolExecutor.AbortPolicy());
      executor.prestartAllCoreThreads();
      this.notificationExecutor = MoreExecutors.listeningDecorator(executor);
      this.notificationQueueSize = new CachedGauge<Integer>(15L, TimeUnit.SECONDS) {
         protected Integer loadValue() { return notifications.size(); }
      };
      SslContextFactory sslContextFactory = new SslContextFactory();
      this.httpClient = new HttpClient(sslContextFactory);
      this.httpClient.setFollowRedirects(false);
      this.httpClient.setConnectTimeout(notificationTimeoutSeconds * 1000L);
      this.httpClient.setCookieStore(new HttpCookieStore.Empty());
      this.notificationTimeoutSeconds = notificationTimeoutSeconds;
      this.acceptCodes = ImmutableSet.copyOf(acceptCodes);
      this.logger = logger.or(new ConsoleLogger());
   }

   /**
    * Enqueue a notification with no authentication.
    * @param targetURL The target URL.
    * @param content The notification content.
    * @return The (listenable) future result.
    */
   public final ListenableFuture<NotificationResult> enqueueNotification(final String targetURL, final ByteString content) {
      return enqueueNotification(new Notification(targetURL, content), ImmutableList.of());
   }

   /**
    * Enqueue a notification with optional basic authentication.
    * @param targetURL The target URL.
    * @param content The notification content.
    * @param headers A collection of headers.
    * @return The (listenable) future result.
    */
   public final ListenableFuture<NotificationResult> enqueueNotification(final String targetURL,
                                                                         final ByteString content,
                                                                         final Collection<Header> headers) {
      return enqueueNotification(new Notification(targetURL, content), headers);
   }

   /**
    * Enqueue a notification for future posting.
    * @param notification The notification.
    * @param headers A colleciton of headers.
    * @return The (listenable) future result.
    */
   public final ListenableFuture<NotificationResult> enqueueNotification(final Notification notification,
                                                                         final Collection<Header> headers) {
      try {
         return notificationExecutor.submit(new NotificationCallable(notification, headers));
      } catch(RejectedExecutionException re) {
         rejectedNotifications.mark();
         return Futures.immediateFailedFuture(re);
      }
   }

   /**
    * A callable for notifications.
    */
   private final class NotificationCallable implements Callable<NotificationResult> {

      NotificationCallable(final Notification notification, final Collection<Header> headers) {
         this.notification = notification;
         this.headers = headers;
      }

      public NotificationResult call() {
         return postNotification(notification, headers);
      }

      private final Notification notification;
      private final Collection<Header> headers;
   }

   private NotificationResult postNotification(final Notification notification, final Collection<Header> headers) {

      Timer.Context ctx = notificationsSent.time();

      try {
         Request request = httpClient.POST(notification.url)
                 .content(new ByteBufferContentProvider(notification.content.asReadOnlyByteBuffer()))
                 .timeout(notificationTimeoutSeconds, TimeUnit.SECONDS);
         headers.forEach(header -> request.header(header.getName(), header.getValue()));
         ContentResponse response = request.send();
         int code = response.getStatus();
         if(acceptCodes.contains(code)) {
            return ACCEPTED_RESULT;
         } else {
            String message = response.getContentAsString();
            return new NotificationResult(code, message, null, notification);
         }
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
         return new NotificationResult(0, "Interrupted while sending", ie, notification);
      } catch(TimeoutException te) {
         notificationErrors.inc();
         return new NotificationResult(0, "Timeout while sending", te, notification);
      } catch(ExecutionException ee) {
         notificationErrors.inc();
         if(ee.getCause() != null) {
            return new NotificationResult(0, "Problem sending", ee.getCause(), notification);
         } else {
            return new NotificationResult(0, "Problem sending", ee, notification);
         }
      } catch(Throwable t) {
         notificationErrors.inc();
         logger.error("Internal error while sending notification", t);
         return new NotificationResult(0, "Internal error", t, notification);
      } finally {
         ctx.stop();
      }
   }

   /**
    * Starts the publisher. Call once before first use.
    * @throws Exception on start error.
    */
   public void start() throws Exception {
      if(isInit.compareAndSet(false, true)) {
         httpClient.start();
      }
   }

   /**
    * Shutdown the publisher.
    * @param maxWaitSeconds The maximum amount of time to wait for in-process notifications to complete.
    * @throws Exception on shutdown error.
    */
   public void shutdown(int maxWaitSeconds) throws Exception {
      shutdown(this.notificationExecutor, this.httpClient, maxWaitSeconds);
   }

   /**
    * Shutdown the publisher.
    * @param notificationExecutor The notification executor.
    * @param httpClient The HTTP client.
    * @param maxWaitSeconds The maximum amount of time to be patient for normal shutdown.
    * @throws Exception on shutdown error.
    */
   private void shutdown(final ListeningExecutorService notificationExecutor,
                         final HttpClient httpClient, final int maxWaitSeconds) throws Exception {
      if(isInit.compareAndSet(true, false)) {
         notificationExecutor.shutdown();
         notificationExecutor.awaitTermination(maxWaitSeconds, TimeUnit.SECONDS);
         if(!notificationExecutor.isShutdown()) {
            notificationExecutor.shutdownNow();
         }
         httpClient.stop();
      }
   }

   /**
    * Gets metrics for registration.
    * @return The metrics.
    */
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("notification-queue-size", notificationQueueSize);
      builder.put("notifications-sent", notificationsSent);
      builder.put("rejected-notifications", rejectedNotifications);
      builder.put("notification-errors", notificationErrors);
      return builder.build();
   }

   /**
    * Is the client initialized?
    */
   private final AtomicBoolean isInit = new AtomicBoolean(false);

   /**
    * The executor handling notifications.
    */
   private final ListeningExecutorService notificationExecutor;

   /**
    * The (cached) size of the notification queue.
    */
   private final CachedGauge<Integer> notificationQueueSize;

   /**
    * Measures timing for notification send.
    */
   private final Timer notificationsSent = new Timer();

   /**
    * Tracks any rejected notifications.
    */
   private final Meter rejectedNotifications = new Meter();

   /**
    * Counts notification errors.
    */
   private final Counter notificationErrors = new Counter();

   /**
    * The notification send timeout.
    */
   private final int notificationTimeoutSeconds;

   /**
    * The HTTP client sending notifications.
    */
   private final HttpClient httpClient;

   /**
    * The HTTP status codes considered to be equivalent to 'Accept'.
    */
   private final ImmutableSet<Integer> acceptCodes;

   /**
    * The logger.
    */
   private final Logger logger;
}
