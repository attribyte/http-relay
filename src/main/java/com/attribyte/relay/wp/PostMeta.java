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

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.attribyte.wp.model.Meta;
import org.attribyte.wp.model.Post;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Comparator;

/**
 * Post metadata (id, timestamp) for serializing state.
 */
public class PostMeta {

   /**
    * Post metadata that represents zero time.
    */
   public static final PostMeta ZERO = new PostMeta(0L, 0L);

   /**
    * Creates post metadata with no fingerprint.
    * @param id The post id.
    * @param lastModifiedMillis The last modified timestamp.
    */
   public PostMeta(final long id, final long lastModifiedMillis) {
      this.id = id;
      this.lastModifiedMillis = lastModifiedMillis;
      this.fingerprint = null;
   }

   /**
    * Creates post metadata with a fingerprint.
    * @param id The post id.
    * @param lastModifiedMillis The last modified timestamp.
    * @param fingerprint The fingerprint.
    */
   public PostMeta(final long id, final long lastModifiedMillis, final HashCode fingerprint) {
      this.id = id;
      this.lastModifiedMillis = lastModifiedMillis;
      this.fingerprint = fingerprint;
   }

   /**
    * Creates post metadata with a fingerprint from a post.
    * @param post The post.
    */
   public PostMeta(final Post post) {
      this(post.id, post.modifiedTimestamp, fingerprint(post));
   }

   /**
    * @return Does this metadata hava a modified time in the future?
    */
   public final boolean isFutureModified() {
      return lastModifiedMillis > System.currentTimeMillis();
   }

   /**
    * Creates a copy of this post meta with the modified timestamp set
    * to the current time.
    * @return The new post meta.
    */
   public PostMeta withCurrentModifiedTime() {
      return new PostMeta(id, System.currentTimeMillis(), fingerprint);
   }

   /**
    * The post id.
    */
   public final long id;

   /**
    * The last modified timestamp.
    */
   public final long lastModifiedMillis;

   /**
    * A hash 'fingerprint' for detecting changes.
    */
   public final HashCode fingerprint;

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this.getClass())
              .add("id", id)
              .add("lastModifiedMillis",lastModifiedMillis)
              .add("lastModifiedHuman", ISODateTimeFormat.basicDateTime().withZone(DateTimeZone.getDefault()).print(lastModifiedMillis))
              .add("lastModifiedHumanUTC", ISODateTimeFormat.basicDateTime().withZoneUTC().print(lastModifiedMillis))
              .add("fingerprint", fingerprint)
              .toString();
   }

   /**
    * Converts this metadata to a byte string (for saving state).
    * @return The byte string.
    */
   public ByteString toBytes() {
      if(fingerprint == null) {
         return ByteString.copyFromUtf8(id + "," + lastModifiedMillis);
      } else {
         return ByteString.copyFromUtf8(id + "," + lastModifiedMillis + "," + fingerprint.toString() + "," +
                 ISODateTimeFormat.basicDateTime().withZoneUTC().print(lastModifiedMillis)
         );
      }
   }

   /**
    * Creates an instance from previously stored bytes.
    * @param bytes The stored bytes.
    * @return The metadata.
    * @throws IOException on invalid format.
    */
   public static PostMeta fromBytes(final ByteString bytes) throws IOException {

      String[] components = bytes.toStringUtf8().split(",");
      if(components.length < 2) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      Long id = Longs.tryParse(components[0].trim());
      if(id == null) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      Long lastModifiedMillis = Longs.tryParse(components[1].trim());
      if(lastModifiedMillis == null) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      if(components.length >= 3) {
         return new PostMeta(id, lastModifiedMillis, HashCode.fromString(components[2].trim()));
      } else {
         return new PostMeta(id, lastModifiedMillis);
      }
   }

   /**
    * Creates a unique fingerprint for a post using the default hasher.
    * @param post The post.
    * @return The fingerprint.
    */
   public static final HashCode fingerprint(final Post post) {
      Hasher hasher = hashFunction.newHasher();
      fingerprint(post, hasher);
      return hasher.hash();
   }

   /**
    * Creates a unique fingerprint for a post.
    * @param post The post.
    * @param hasher The hasher.
    */
   public static final void fingerprint(final Post post, final Hasher hasher) {
      if(post == null) {
         return;
      }

      hasher.putString(Strings.nullToEmpty(post.slug), Charsets.UTF_8);
      hasher.putString(Strings.nullToEmpty(post.title), Charsets.UTF_8);
      hasher.putString(Strings.nullToEmpty(post.excerpt), Charsets.UTF_8);
      hasher.putString(Strings.nullToEmpty(post.content), Charsets.UTF_8);
      hasher.putLong(post.authorId);
      hasher.putLong(post.publishTimestamp);
      hasher.putString(post.status != null ? post.status.toString() : "", Charsets.UTF_8);
      hasher.putString(Strings.nullToEmpty(post.guid), Charsets.UTF_8);
      post.metadata.stream().sorted(Comparator.comparing(o -> o.key)).forEach(meta -> {
         hasher.putString(meta.key, Charsets.UTF_8).putString(Strings.nullToEmpty(meta.value), Charsets.UTF_8);
      });
      hasher.putString(post.type != null ? post.type.toString() : "", Charsets.UTF_8);
      hasher.putString(Strings.nullToEmpty(post.mimeType), Charsets.UTF_8);
      post.tags().stream().sorted(Comparator.comparing(o -> o.term.name)).forEach(term -> {
         hasher.putString(term.term.name, Charsets.UTF_8);
      });
      post.categories().stream().sorted(Comparator.comparing(o -> o.term.name)).forEach(term -> {
         hasher.putString(term.term.name, Charsets.UTF_8);
      });
      post.children.forEach(childPost -> fingerprint(childPost, hasher));
   }

   /**
    * The hash function.
    */
   public static final HashFunction hashFunction = Hashing.sha256();
}
