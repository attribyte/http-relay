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
import com.attribyte.relay.DusterClient;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AsyncClient;
import org.attribyte.api.http.impl.jetty.JettyClient;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.relay.Message;
import org.attribyte.relay.RDBSupplier;
import org.attribyte.relay.util.MessageUtil;
import org.attribyte.util.InitUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.attribyte.relay.HTMLUtil.extractLinks;

/**
 * Extracts posts from a Wordpress database as Attribyte
 * binary replication (proto) format.
 *
 * <dl>
 *    <dt>siteId</dt>
 *    <dd>The wordpress site id.</dd>
 *
 *    <dt>namespace</dt>
 *    <dd>The namespace string added to post, author, etc. ids</dd>
 *
 *    <dt>commentNamespace</dt>
 *    <dd>The namespace string added to comment ids.</dd>
 *
 *    <dt>selectSleepMillis</dt>
 *    <dd>The time to sleep between selects, in milliseconds.</dd>
 *
 *    <dt>maxSelected</dt>
 *    <dd>The maximum number of posts/comments selected</dd>
 *
 *    <dt>allowedStatus</dt>
 *    <dd>A comma-separated list of status allowed for replication.</dd>
 *
 *    <dt>allowedTypes</dt>
 *    <dd>A comma-separated list of post types allowed for replication.</dd>
 *
 *    <dt>stopOnLostMessage</dt>
 *    <dd>Stops the relay on any lost message report.</dd>
 * </dl>
 */
public class WPSupplier extends RDBSupplier {

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {

      if(isInit.compareAndSet(false, true)) {

         this.logger = logger;

         String namespace = props.getProperty("namespace");
         String commentNamespace = props.getProperty("commentNamespace", "comment");

         long siteId = Long.parseLong(props.getProperty("siteId", "0"));
         if(siteId < 1L) {
            throw new Exception("A 'siteId' must be specified");
         }

         Properties siteProps = new InitUtil("site.", props, false).getProperties();
         SiteMeta overrideSiteMeta = new SiteMeta(siteProps);

         initPools(props, logger);
         this.db = new DB(defaultConnectionPool, namespace, commentNamespace, siteId, overrideSiteMeta);

         if(savedState.isPresent()) {
            startMeta = PostMeta.fromBytes(savedState.get());
         } else {
            startMeta = PostMeta.ZERO;
         }

         this.selectSleepMillis = Integer.parseInt(props.getProperty("selectSleepMillis", "30000"));
         this.maxSelected = Integer.parseInt(props.getProperty("maxSelected", "500"));

         String allowedStatusStr = props.getProperty("allowedStatus", "").trim();
         if(!allowedStatusStr.isEmpty()) {
            this.allowedStatus = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(allowedStatusStr));
         } else {
            this.allowedStatus = DEFAULT_ALLOWED_STATUS;
         }

         String allowedTypesStr = props.getProperty("allowedTypes", "").trim();
         if(!allowedStatusStr.isEmpty()) {
            this.allowedTypes = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(allowedTypesStr));
         } else {
            this.allowedStatus = DEFAULT_ALLOWED_TYPES;
         }

         this.stopOnLostMessage = props.getProperty("stopOnLostMessage", "false").equalsIgnoreCase("true");

         Properties dusterProps = new InitUtil("duster.", props, false).getProperties();
         if(dusterProps.size() > 0) {
            InitUtil httpProps = new InitUtil("http.", props);
            this.httpClient = new JettyClient();
            this.httpClient.init("http.", props, logger);
            this.dusterClient = new DusterClient(dusterProps, httpClient, logger);
         }
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
         if(httpClient != null) {
            try {
               httpClient.shutdown();
            } catch(Exception e) {
               logger.error("Problem shutting down HTTP client", e);
            }
         }
      }
      return Optional.of(this.startMeta.toBytes());
   }

   /**
    * Builds the replication message.
    * @return The message or {@code absent} if no new entries.
    * @throws SQLException on database error.
    */
   protected Optional<Message> buildMessage() throws SQLException {

      List<PostMeta> nextMeta = db.selectModifiedPosts(Optional.of(startMeta), maxSelected);
      logger.info(String.format("Selected %d modified posts", nextMeta.size()));

      if(nextMeta.isEmpty()) {
         return Optional.absent();
      }

      Collections.sort(nextMeta); //Ensure the correct ascending order...

      Set<Long> allEntries = Sets.newHashSetWithExpectedSize(maxSelected > 1024 ? 1024 : maxSelected);
      Set<Long> allAuthors = Sets.newHashSetWithExpectedSize(maxSelected > 1024 ? 1024 : maxSelected);

      ClientProtos.WireMessage.Replication.Builder replicationMessage = ClientProtos.WireMessage.Replication.newBuilder();
      replicationMessage.addSites(db.parentSite);
      replicationMessage.addSources(db.parentSource);
      replicationMessage.setOrigin(MessageUtil.buildServerOrigin());

      for(PostMeta meta : nextMeta) {
         if(!allEntries.contains(meta.id)) {
            allEntries.add(meta.id);
            if(allowedStatus.contains(meta.status) && allowedTypes.contains(meta.type)) {
               ClientProtos.WireMessage.Entry.Builder entry =
                       db.selectPost(meta.id).or(
                               ClientProtos.WireMessage.Entry.newBuilder()
                                       .setUID(db.buildId(meta.id))
                                       .setDeleted(true)
                       );
               if(!entry.getDeleted()) {
                  UserMeta user = db.resolveUser(meta.authorId).or(new UserMeta(meta.authorId, "Author_" + meta.authorId, "author_" + meta.authorId));
                  ClientProtos.WireMessage.Author author =
                          ClientProtos.WireMessage.Author.newBuilder(entry.getAuthor()).setName(user.displayName).build();
                  if(!allAuthors.contains(meta.authorId)) {
                     allAuthors.add(meta.authorId);
                     replicationMessage.addAuthors(author);
                  }

                  List<TermMeta> tags = db.resolvePostTerms(meta.id, "tag");
                  for(TermMeta tag : tags) {
                     entry.addTag(tag.name);
                  }

                  List<TermMeta> categories = db.resolvePostTerms(meta.id, "category");
                  for(TermMeta category : categories) {
                     entry.addTopic(category.name);
                  }

                  TermMeta firstTag = tags.size() > 0 ? tags.get(0) : categories.size() > 0 ? categories.get(0) : null;
                  entry.setCanonicalLink(
                          db.siteMeta.buildPermalink(entry, meta.postName, user.niceName, firstTag != null ? firstTag.slug : "unclassified")
                  );

                  //Extracts links (to citations) and images.
                  extractLinks(entry, db.siteMeta.baseURL);

                  //Enable duster images and transforms, if configured
                  if(dusterClient != null) {
                     dusterClient.enableImages(entry);
                  }
               }

               replicationMessage.addEntries(entry.build());

            } else {
               replicationMessage.addEntriesBuilder()
                       .setUID(db.buildId(meta.id))
                       .setDeleted(true);
            }
         }
      }

      String messageId = this.startMeta.toBytes().toStringUtf8();
      this.startMeta = nextMeta.get(nextMeta.size() - 1);

      System.out.println(TextFormat.printToString(replicationMessage.build()));

      return Optional.of(Message.publish(messageId, replicationMessage.build().toByteString()));
   }

   @Override
   public void lostMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      lostMessages.inc();
      logger.error("Lost message: " + message.id);
      if(stopOnLostMessage) {
         shutdown();
      }
   }

   @Override
   public void completedMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      completedMessages.inc();
      logger.info("Completed message: " + message.id);
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
    * The set of allowed status values.
    */
   private Set<String> allowedStatus;

   /**
    * The set of allowed post types.
    */
   private Set<String> allowedTypes;

   /**
    * Is the supplier initialized?
    */
   private final AtomicBoolean isInit = new AtomicBoolean(false);

   /**
    * Should replication be stopped if any lost message is detected?
    */
   private boolean stopOnLostMessage;

   /**
    * The duster client, if configured.
    */
   private DusterClient dusterClient;

   /**
    * The HTTP client, if required.
    */
   private AsyncClient httpClient;

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
    * <p>
    *    Update <em>only</em> in {@code nextMessage}!
    * </p>
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

   /**
    * The default set of allowed post status ('publish' only).
    * @see <a href="https://codex.wordpress.org/Post_Status">https://codex.wordpress.org/Post_Status</a>
    */
   static final ImmutableSet<String> DEFAULT_ALLOWED_STATUS = ImmutableSet.of("publish");

   /**
    * The default set of allowed post type ('post' only).
    * @see <a href="https://codex.wordpress.org/Post_Types">https://codex.wordpress.org/Post_Types</a>
    */
   static final ImmutableSet<String> DEFAULT_ALLOWED_TYPES = ImmutableSet.of("post");

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("db", defaultConnectionPool.getMetrics());
      builder.put("message-builds", messageBuilds);
      builder.put("completed-messages", completedMessages);
      builder.put("lost-messages", lostMessages);
      builder.put("time-to-acknowledge", timeToAcknowledge);
      return builder.build();
   }
}