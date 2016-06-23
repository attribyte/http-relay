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

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Post metadata.
 */
public class PostMeta implements Comparable<PostMeta> {

   /**
    * Post metadata that represents zero time.
    */
   public static final PostMeta ZERO = new PostMeta(0L, 0L, "", "");

   /**
    * Creates post metadata.
    * @param id The post id.
    * @param lastModifiedMillis The last modified timestamp.
    * @param status The post status string.
    * @param type The post type string.
    */
   public PostMeta(final long id, final long lastModifiedMillis, final String status, final String type) {
      this.id = id;
      this.lastModifiedMillis = lastModifiedMillis;
      this.status = status;
      this.type = type;
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
    * The post status string.
    */
   public final String status;

   /**
    * The post type string.
    */
   public final String type;

   @Override
   public int compareTo(final PostMeta other) {
      if(this.lastModifiedMillis == other.lastModifiedMillis) {
         return this.id < other.id ? -1 : this.id > other.id ? 1 : 0;
      } else {
         return this.lastModifiedMillis < other.lastModifiedMillis ? -1 : this.lastModifiedMillis > other.lastModifiedMillis ? 1 : 0;
      }
   }

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this.getClass())
              .add("id", id)
              .add("lastModifiedMillis",lastModifiedMillis)
              .add("status", status)
              .add("type", type).toString();
   }

   /**
    * Converts this metadata to a byte string (for saving state).
    * @return The byte string.
    */
   public ByteString toBytes() {
      return ByteString.copyFromUtf8(id + "," + lastModifiedMillis + "," + status  + "," + type);
   }

   /**
    * Creates an instance from previously stored bytes.
    * @param bytes The stored bytes.
    * @return The metadata.
    * @throws IOException on invalid format.
    */
   public static PostMeta fromBytes(final ByteString bytes) throws IOException {
      String[] components = bytes.toStringUtf8().split(",");
      if(components.length < 4) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      Long id = Longs.tryParse(components[0]);
      if(id == null) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      Long lastModifiedMillis = Longs.tryParse(components[1]);
      if(lastModifiedMillis == null) {
         throw new IOException(String.format("Invalid state: '%s'", bytes.toStringUtf8()));
      }

      return new PostMeta(id, lastModifiedMillis, components[2], components[3]);
   }
}
