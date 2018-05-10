/*
 * Copyright 2018 Attribyte, LLC
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

package com.attribyte.relay.feed;

import com.attribyte.client.ClientProtos;
import com.attribyte.parser.ContentCleaner;
import com.attribyte.parser.ParseResult;
import com.attribyte.parser.Parser;
import com.attribyte.parser.entry.UniversalParser;
import com.attribyte.parser.model.Author;
import com.attribyte.parser.model.Entry;
import com.attribyte.parser.model.Image;
import com.codahale.metrics.Metric;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.attribyte.api.Logger;
import org.attribyte.relay.Message;
import org.attribyte.relay.Supplier;
import org.attribyte.relay.Transformer;
import org.attribyte.relay.util.MessageUtil;
import org.attribyte.util.InitUtil;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Parses a response as a feed and transforms to
 * Attribyte binary replication (proto) format.
 */
public class FeedTransformer implements Transformer {

   @Override
   public Message transform(Message message) {

      if(message.isPublish()) {
         ClientProtos.WireMessage.Replication.Builder replicationMessage = ClientProtos.WireMessage.Replication.newBuilder();
         replicationMessage.setOrigin(MessageUtil.buildServerOrigin(originId));

         ParseResult result = parser.parse(message.message.toStringUtf8(), message.id, contentCleaner);
         if(result.hasErrors()) {
            replicationMessage.setError(
                    ClientProtos.WireMessage.Error.newBuilder()
                            .setInternalMessage(
                                    Joiner.on(System.lineSeparator())
                                            .join(result.errors.stream()
                                            .map(Object::toString)
                                            .iterator())
                            )
            );
         } else {
            result.resource.ifPresent(resource  -> {
               Map<String, ClientProtos.WireMessage.Author> authors = Maps.newHashMap();
               Set<String> sites = Sets.newHashSet();
               resource.entries.forEach(entry -> {
                  ClientProtos.WireMessage.Entry.Builder builder = ClientProtos.WireMessage.Entry.newBuilder();

                  id(entry).ifPresent(builder::setId);

                  if(!Strings.isNullOrEmpty(entry.canonicalLink)) {
                     builder.setCanonicalLink(entry.canonicalLink);
                  }

                  entry.altLinks.forEach(builder::addAltLink);

                  if(!Strings.isNullOrEmpty(entry.title)) {
                     builder.setTitle(entry.title);
                  }

                  if(!Strings.isNullOrEmpty(entry.summary)) {
                     builder.setSummary(entry.summary);
                  }

                  if(!Strings.isNullOrEmpty(entry.cleanContent)) {
                     builder.setContent(entry.cleanContent);
                  }

                  if(entry.publishedTimestamp > 0L) {
                     builder.setPublishTimeMillis(entry.publishedTimestamp);
                  }

                  if(entry.updatedTimestamp > 0L) {
                     builder.setLastModifiedMillis(entry.updatedTimestamp);
                  }

                  entry.tags.forEach(builder::addTag);

                  entry.primaryImage.ifPresent(primaryImage -> {
                     builder.setPrimaryImage(toImage(primaryImage));
                  });

                  entry.images.forEach(image -> {
                     builder.addImages(toImage(image));
                  });

                  site(entry).ifPresent(site -> {
                     builder.setParentSite(site);
                     if(!sites.contains(site.getUrl())) {
                        sites.add(site.getUrl());
                        replicationMessage.addSites(site);
                     }
                  });

                  if(!entry.authors.isEmpty()) {
                     Author author = entry.authors.get(0);
                     String username = Strings.nullToEmpty(author.id);
                     if(username.isEmpty()) {
                        username = Strings.nullToEmpty(author.name);
                     }

                     if(!username.isEmpty()) {
                        ClientProtos.WireMessage.Author entryAuthor =
                                authors.computeIfAbsent(username, un ->
                                        ClientProtos.WireMessage.Author.newBuilder().setUsername(un).build());
                        builder.setAuthor(entryAuthor);
                     }
                  }
               });

               replicationMessage.addAllAuthors(authors.values());

            });
         }


         return Message.publish(message.id, replicationMessage.build().toByteString());

      } else {
         return message;
      }
   }

   protected ClientProtos.WireMessage.Image.Builder toImage(final Image image) {
      ClientProtos.WireMessage.Image.Builder builder = ClientProtos.WireMessage.Image.newBuilder();

      if(!Strings.isNullOrEmpty(image.link)) {
         builder.setOriginalSrc(image.link);
      }

      if(image.width > 0) {
         builder.setWidth(image.width);
      }

      if(image.height > 0) {
         builder.setHeight(image.height);
      }

      if(!Strings.isNullOrEmpty(image.altText)) {
         builder.setAltText(image.altText);
      }

      if(!Strings.isNullOrEmpty(image.title)) {
         builder.setTitle(image.title);
      }

      return builder;
   }

   @Override
   public void init(final Supplier supplier,
                    final Properties props,
                    final Logger logger) throws Exception {

      this.logger = logger;

      this.originId = props.getProperty("originId", "");

      String parserClass = props.getProperty("parserClass", "");
      if(!parserClass.isEmpty()) {
         parser = (Parser)Class.forName(parserClass).newInstance();
      } else {
         parser = new UniversalParser();
      }

      String contentCleanerClass = props.getProperty("contentCleanerClass", "");
      if(!contentCleanerClass.isEmpty()) {
         contentCleaner = (ContentCleaner)Class.forName(contentCleanerClass).newInstance();
         contentCleaner.init(new InitUtil("contentCleaner.", props, false).getProperties());
      } else {
         contentCleaner = ContentCleaner.NOOP;
      }

      String defaultSiteURL = props.getProperty("defaultSiteURL", "");
      String defaultSiteTitle = props.getProperty("defaultSiteTitle", "");
      String defaultSiteDescription = props.getProperty("defaultSiteDescription", "");

      if(!defaultSiteURL.isEmpty()) {
         ClientProtos.WireMessage.Site.Builder builder = ClientProtos.WireMessage.Site.newBuilder().setUrl(defaultSiteURL);
         if(!defaultSiteTitle.isEmpty()) {
            builder.setTitle(defaultSiteTitle);
         }
         if(!defaultSiteDescription.isEmpty()) {
            builder.setDescription(defaultSiteDescription);
         }
         this.defaultSite = Optional.of(builder.build());
      } else {
         this.defaultSite = Optional.empty();
      }
   }

   @Override
   public void shutdown() {

   }

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.of();
   }

   /**
    * Gets the site associated with an entry, if any.
    * @param entry The entry.
    * @return The site, if any.
    */
   protected Optional<ClientProtos.WireMessage.Site> site(Entry entry) {
      return defaultSite;
   }

   /**
    * Gets the numeric id associated with an entry, if any.
    * @param entry The entry.
    * @return The numeric id, if any.
    */
   protected Optional<Long> id(Entry entry) {
      return Optional.empty();
   }

   /**
    * The parser.
    */
   private Parser parser;

   /**
    * The content cleaner.
    */
   private ContentCleaner contentCleaner;

   /**
    * The default site associated with entries, if any.
    */
   private Optional<ClientProtos.WireMessage.Site> defaultSite;

   /**
    * The origin id for replication messages.
    */
   private String originId;

   /**
    * The logger.
    */
   private Logger logger;

}