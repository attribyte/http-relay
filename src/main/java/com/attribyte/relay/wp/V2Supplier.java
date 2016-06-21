package com.attribyte.relay.wp;

import com.attribyte.client.ClientProtos;
import com.attribyte.relay.DusterClient;
import com.codahale.metrics.Metric;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Request;
import org.attribyte.relay.HTTPSupplier;
import org.attribyte.relay.Message;
import org.attribyte.util.InitUtil;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class V2Supplier extends HTTPSupplier {

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {
      super.init(props, savedState, logger);

      this.namespace = props.getProperty("namespace");

      String authorCacheTime = props.getProperty("authorCacheTime", "30m");
      long cacheTimeMillis = InitUtil.millisFromTime(authorCacheTime);
      if(cacheTimeMillis < 1) {
         cacheTimeMillis = InitUtil.millisFromTime("30m");
      }

      this.authorCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(cacheTimeMillis, TimeUnit.MILLISECONDS)
              .build();

      Properties dusterProps = new InitUtil("duster.", props, false).getProperties();
      if(dusterProps.size() > 0) {
         this.dusterClient = new DusterClient(dusterProps, httpClient, logger);
      } else {
         this.dusterClient = null;
      }
   }

   @Override
   protected void initState(Optional<ByteString> state) {
   }

   @Override
   protected Optional<Request> nextRequest() {
      return Optional.absent();
   }

   @Override
   protected Optional<ByteString> nextState(final Message completedMessage) {
      return Optional.absent();
   }

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> metrics = ImmutableMap.builder();
      metrics.putAll(super.getMetrics());
      return metrics.build();
   }

   /**
    * Translates a WP post to an entry.
    * @param jsonPost The WP post as JSON.
    * @param builder The entry builder.
    */
   private void toProto(final ObjectNode jsonPost, ClientProtos.WireMessage.Entry.Builder builder) {

      JsonNode idNode = jsonPost.findPath("id");
      if(idNode.isValueNode()) {
         ClientProtos.WireMessage.Id.Builder id = ClientProtos.WireMessage.Id.newBuilder();
         id.setId(idNode.textValue());
         if(namespace != null) {
            id.setNamespace(namespace);
         }
         builder.setUID(id);
      }

      JsonNode publishTimeNode = jsonPost.findPath("date_gmt");
      if(publishTimeNode.isTextual()) {
         builder.setPublishTimeMillis(ISO8601_PARSER.parseMillis(publishTimeNode.textValue()));
      }

      JsonNode modTimeNode = jsonPost.findPath("modified_gmt");
      if(modTimeNode.isTextual()) {
         builder.setLastModifiedMillis(ISO8601_PARSER.parseMillis(modTimeNode.textValue()));
      }

      JsonNode linkNode = jsonPost.findPath("link");
      if(linkNode.isTextual()) {
         builder.setCanonicalLink(linkNode.textValue());
      }

      String title = rendered(jsonPost, "title");
      if(title != null) {
         builder.setTitle(title);
      }

      String excerpt = rendered(jsonPost, "excerpt");
      if(excerpt != null) {
         builder.setSummary(excerpt);
      }

      String content = rendered(jsonPost, "content");
      if(content != null) {
         builder.setContent(content);
      }

      JsonNode tagsNode = jsonPost.findPath("tags");
      if(tagsNode.isArray()) {
         tagsNode.forEach(node -> {
            if(node.isTextual()) builder.addTag(node.textValue());
         });
      }

      JsonNode categoriesNode = jsonPost.findPath("categories"); //Categories translate to "topic"
      if(tagsNode.isArray()) {
         tagsNode.forEach(node -> {
            if(node.isTextual()) builder.addTopic(node.textValue());
         });
      }

      JsonNode authorIdNode = jsonPost.findPath("author");

   }

   /**
    * Gets the {@code rendered} value for a node.
    * @param jsonPost The parent post.
    * @param nodeName The name of the node.
    * @return The {@code rendered} content or {@code null}.
    */
   private String rendered(final ObjectNode jsonPost, final String nodeName) {
      JsonNode parentNode = jsonPost.findPath(nodeName);
      if(parentNode.isMissingNode()) {
         return null;
      }
      JsonNode contentNode = parentNode.findPath("rendered");
      return contentNode.isTextual() ? contentNode.textValue() : null;
   }

   /**
    * The namespace for all ids.
    */
   private String namespace;

   /**
    * A cache of authors.
    */
   private Cache<Long, ClientProtos.WireMessage.Author> authorCache;

   /**
    * A parser for the ISO8601 format.
    */
   private static final DateTimeFormatter ISO8601_PARSER = ISODateTimeFormat.dateTimeParser();

   /**
    * The duster client. May be {@code null}.
    */
   private DusterClient dusterClient;

}
