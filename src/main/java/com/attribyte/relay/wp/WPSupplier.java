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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AsyncClient;
import org.attribyte.api.http.impl.jetty.JettyClient;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.relay.Message;
import org.attribyte.relay.RDBSupplier;
import org.attribyte.relay.util.MessageUtil;
import org.attribyte.util.InitUtil;
import org.attribyte.wp.db.DB;
import org.attribyte.wp.model.ImageAttachment;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Post;
import org.attribyte.wp.model.Site;
import org.attribyte.wp.model.TaxonomyTerm;
import org.attribyte.wp.model.User;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.attribyte.relay.HTMLUtil.extractLinks;
import static org.attribyte.wp.Util.TAG_TAXONOMY;
import static org.attribyte.wp.Util.CATEGORY_TAXONOMY;

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
 *    <dt>allowedMeta</dt>
 *    <dd>A comma-separated list of metadata names to replicate with posts.</dd>
 *
 *    <dt>stopOnLostMessage</dt>
 *    <dd>Stops the relay on any lost message report.</dd>
 *
 *    <dt>originId</dt>
 *    <dd>The origin id sent with replicated messages.</dd>
 *
 *    <dt>contentTransformer</dt>
 *    <dd>A class that implements {@code ContentTransformer}.
 *    Must have a default constructor.
 *    </dd>
 *
 *    <dt>cleanShortcodes</dt>
 *    <dd>If {@code true}, shortcodes will be removed from content. Ignored
 *    if {@code contentTransfomer} is specified. Default is {@code true}.</dd>
 *
 * </dl>
 */
public class WPSupplier extends RDBSupplier {

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {

      if(isInit.compareAndSet(false, true)) {

         this.logger = logger;

         logger.info("Initializing WP supplier...");

         final long siteId = Long.parseLong(props.getProperty("siteId", "0"));
         if(siteId < 1L) {
            throw new Exception("A 'siteId' must be specified");
         }

         final Set<String> cachedTaxonomies = ImmutableSet.of(TAG_TAXONOMY, CATEGORY_TAXONOMY);
         final Duration taxonomyCacheTimeout = Duration.ofMinutes(30); //TODO: Configure
         final Duration userCacheTimeout = Duration.ofMinutes(30); //TODO: Configure
         initPools(props, logger);
         this.db = new DB(defaultConnectionPool, siteId, cachedTaxonomies, taxonomyCacheTimeout, userCacheTimeout);

         Properties siteProps = new InitUtil("site.", props, false).getProperties();
         Site overrideSite = new Site(siteProps);
         this.site = this.db.selectSite().overrideWith(overrideSite);

         logger.info(String.format("Initialized site %d - %s", this.site.id, Strings.nullToEmpty(this.site.title)));

         if(savedState.isPresent()) {
            startMeta = PostMeta.fromBytes(savedState.get());
         } else {
            startMeta = PostMeta.ZERO;
         }

         this.selectSleepMillis = Integer.parseInt(props.getProperty("selectSleepMillis", "30000"));
         this.maxSelected = Integer.parseInt(props.getProperty("maxSelected", "500"));

         String allowedStatusStr = props.getProperty("allowedStatus", "").trim();
         if(!allowedStatusStr.isEmpty()) {
            this.allowedStatus = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(allowedStatusStr)
                    .stream().map(Post.Status::fromString).collect(Collectors.toSet()));
         } else {
            this.allowedStatus = DEFAULT_ALLOWED_STATUS;
         }

         String allowedTypesStr = props.getProperty("allowedTypes", "").trim();
         if(!allowedStatusStr.isEmpty()) {
            this.allowedTypes = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(allowedTypesStr))
                    .stream().map(Post.Type::fromString).collect(Collectors.toSet());
         } else {
            this.allowedTypes = DEFAULT_ALLOWED_TYPES;
         }

         String allowedMetaStr = props.getProperty("allowedMeta", "").trim();
         if(!allowedMetaStr.isEmpty()) {
            this.allowedMeta = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(allowedMetaStr))
                    .stream().collect(Collectors.toSet());
         } else {
            this.allowedMeta = ImmutableSet.of();
         }

         this.stopOnLostMessage = props.getProperty("stopOnLostMessage", "false").equalsIgnoreCase("true");
         this.originId = props.getProperty("originId", "");

         Properties dusterProps = new InitUtil("duster.", props, false).getProperties();
         if(dusterProps.size() > 0) {
            this.httpClient = new JettyClient();
            this.httpClient.init("http.", props, logger);
            this.dusterClient = new DusterClient(dusterProps, httpClient, logger);
            logger.info("Duster is enabled...");
         } else {
            logger.info("Duster is disabled...");
         }

         if(!props.getProperty("contentTransformer", "").trim().isEmpty()) {
            this.contentTransformer = (ContentTransformer)(Class.forName(props.getProperty("contentTransformer")).newInstance());
            this.contentTransformer.init(props);
         } else if(props.getProperty("cleanShortcodes", "true").equalsIgnoreCase("true")) {
            this.contentTransformer = new ShortcodeCleaner();
         }

         logger.info("Initialized WP supplier...");

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

      List<Post> nextPosts = Lists.newArrayListWithExpectedSize(maxSelected > 1024 ? 1024 : maxSelected);
      for(Post.Type type : allowedTypes) {
         final Timer.Context ctx = postSelects.time();
         try {
            nextPosts.addAll(db.selectModifiedPosts(type, startMeta.lastModifiedMillis, startMeta.id, maxSelected, true));  //Resolves users, meta, etc.
         } finally {
            ctx.stop();
         }
      }
      Collections.sort(nextPosts, ascendingPostComparator);

      logger.info(String.format("Selected %d modified posts", nextPosts.size()));

      if(nextPosts.isEmpty()) {
         return Optional.absent();
      }


      final Timer.Context ctx = messageBuilds.time();

      try {

         Set<Long> allEntries = Sets.newHashSetWithExpectedSize(maxSelected > 1024 ? 1024 : maxSelected);
         Set<Long> allAuthors = Sets.newHashSetWithExpectedSize(maxSelected > 1024 ? 1024 : maxSelected);

         ClientProtos.WireMessage.Replication.Builder replicationMessage = ClientProtos.WireMessage.Replication.newBuilder();

         ClientProtos.WireMessage.Site parentSite =
                 ClientProtos.WireMessage.Site.newBuilder()
                         .setTitle(site.title)
                         .setDescription(site.description)
                         .setUrl(site.baseURL).build();

         replicationMessage.addSites(parentSite);

         replicationMessage.setOrigin(MessageUtil.buildServerOrigin(originId));

         for(Post post : nextPosts) {
            if(!allEntries.contains(post.id)) {
               allEntries.add(post.id);
               if(allowedStatus.contains(post.status)) {
                  if(post.status == Post.Status.PUBLISH) {
                     if(post.author == null) {
                        logger.error(String.format("Skipping post with missing author (%d)", post.id));
                        continue;
                     }

                     ClientProtos.WireMessage.Author author = fromUser(post.author);
                     if(!allAuthors.contains(author.getId())) {
                        allAuthors.add(author.getId());
                        replicationMessage.addAuthors(author);
                     }

                     ClientProtos.WireMessage.Entry.Builder entry = ClientProtos.WireMessage.Entry.newBuilder();
                     entry.setId(post.id);
                     entry.setPermanent(true);
                     entry.setAuthor(author);
                     entry.setParentSite(parentSite);

                     entry.setCanonicalLink(site.buildPermalink(post));

                     if(post.title != null) {
                        entry.setTitle(post.title);
                     }

                     if(post.excerpt != null) {
                        entry.setSummary(post.excerpt);
                     }

                     if(post.content != null) {
                        entry.setContent(contentTransformer == null ? post.content : contentTransformer.transform(post.content));
                     }

                     if(post.publishTimestamp > 0L) {
                        entry.setPublishTimeMillis(post.publishTimestamp);
                     }

                     if(post.modifiedTimestamp > 0L) {
                        entry.setLastModifiedMillis(post.modifiedTimestamp);
                     }

                     long featuredImageId = featuredImageId(post.metadata);

                     for(Post attachmentPost : post.children) {
                        if(attachmentPost.type == Post.Type.ATTACHMENT && isImageAttachment(attachmentPost)) {
                           if(featuredImageId > 0 && attachmentPost.id == featuredImageId) {
                              entry.setPrimaryImage(
                                      ClientProtos.WireMessage.Image.newBuilder()
                                              .setOriginalSrc(new ImageAttachment(attachmentPost).path())
                                              .setTitle(Strings.nullToEmpty(attachmentPost.excerpt))
                              );
                           } else {
                              entry.addImagesBuilder()
                                      .setOriginalSrc(new ImageAttachment(attachmentPost).path())
                                      .setTitle(Strings.nullToEmpty(attachmentPost.excerpt));
                           }
                        }
                     }

                     extractLinks(entry, site.baseURL); //Other images, citations...

                     if(dusterClient != null) {
                        dusterClient.enableImages(entry);
                     }


                     for(TaxonomyTerm tag : post.tags()) {
                        entry.addTag(tag.term.name);
                     }
                     for(TaxonomyTerm category : post.categories()) {
                        entry.addTopic(category.term.name);
                     }

                     if(allowedMeta.size() > 0 && post.metadata != null && post.metadata.size() > 0) {
                        for(Meta meta : post.metadata) {
                           entry.addMetaBuilder().setName(meta.key).setValue(meta.value);
                        }
                     }

                     replicationMessage.addEntries(entry.build());

                  } else {
                     replicationMessage.addEntriesBuilder()
                             .setId(post.id)
                             .setDeleted(true);
                  }
               } else {
                  replicationMessage.addEntriesBuilder()
                          .setId(post.id)
                          .setDeleted(true);
               }
            }
         }

         String messageId = this.startMeta.toBytes().toStringUtf8();
         Post lastPost = nextPosts.get(nextPosts.size() - 1);
         this.startMeta = new PostMeta(lastPost.id, lastPost.modifiedTimestamp);
         return Optional.of(Message.publish(messageId, replicationMessage.build().toByteString()));
      } finally {
         ctx.stop();
      }
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
    * The parent site for all posts.
    */
   private Site site;

   /**
    * The metadata start (timestamp, id) for the next request.
    */
   private PostMeta startMeta;

   /**
    * The set of allowed status values.
    */
   private Set<Post.Status> allowedStatus;

   /**
    * The set of allowed post types.
    */
   private Set<Post.Type> allowedTypes;

   /**
    * The set of metadata to be replicated with posts.
    */
   private Set<String> allowedMeta;

   /**
    * The origin id sent with messages.
    */
   private String originId;

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
    * An optional content transformer.
    */
   private ContentTransformer contentTransformer;

   /**
    * Time to build published messages.
    */
   private final Timer messageBuilds = new Timer();

   /**
    * Time to select modified posts.
    */
   private final Timer postSelects = new Timer();

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
   static final ImmutableSet<Post.Status> DEFAULT_ALLOWED_STATUS = ImmutableSet.of(Post.Status.PUBLISH);

   /**
    * The default set of allowed post type ('post' only).
    * @see <a href="https://codex.wordpress.org/Post_Types">https://codex.wordpress.org/Post_Types</a>
    */
   static final ImmutableSet<Post.Type> DEFAULT_ALLOWED_TYPES = ImmutableSet.of(Post.Type.POST);

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("db", defaultConnectionPool.getMetrics());
      builder.put("message-builds", messageBuilds);
      builder.put("post-selects", postSelects);
      builder.put("completed-messages", completedMessages);
      builder.put("lost-messages", lostMessages);
      builder.put("time-to-acknowledge", timeToAcknowledge);
      return builder.build();
   }

   /**
    * Sort posts in ascending order by time, id.
    */
   static final Comparator<Post> ascendingPostComparator = (o1, o2) -> {
      final int c = Long.compare(o1.modifiedTimestamp, o2.modifiedTimestamp);
      return c != 0 ? c : Long.compare(o1.id, o2.id);
   };

   /**
    * Creates a wire author from a WP user.
    * @param user The user.
    * @return The wire author.
    */
   private static ClientProtos.WireMessage.Author fromUser(final User user) {
      ClientProtos.WireMessage.Author.Builder author = ClientProtos.WireMessage.Author.newBuilder();
      if(!Strings.isNullOrEmpty(user.displayName)) {
         author.setName(user.displayName);
      }
      if(!Strings.isNullOrEmpty(user.username)) {
         author.setUsername(user.username);
      }
      if(user.id > 0) {
         author.setId(user.id);
      }
      return author.build();
   }

   /**
    * Gets the id of the featured image from post metadata.
    * @param meta The post metadata.
    * @return The id of the featured image or {@code 0} if none.
    */
   private static long featuredImageId(final List<Meta> meta) {
      for(Meta m : meta) {
         if(m.key.equals("_thumbnail_id")) {
            Long id = Longs.tryParse(m.value);
            if(id != null) {
               return id;
            }
         }
      }
      return 0;
   }

   /**
    * Check the mime type of an attachment to determine if it is an image.
    * @param post The post.
    * @return Is the attachment an image?
    */
   private static boolean isImageAttachment(final Post post) {
      return imageMimeTypes.contains(Strings.nullToEmpty(post.mimeType));
   }

   /**
    * The default set of mime types allowed for images.
    */
   private static ImmutableSet<String> imageMimeTypes = ImmutableSet.of(
           "image/jpeg",
           "image/jpg",
           "image/png",
           "image/gif"
   );
}