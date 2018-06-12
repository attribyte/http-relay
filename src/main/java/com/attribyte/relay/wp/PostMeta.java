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
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 * Post metadata (id, timestamp) for serializing state.
 */
public class PostMeta {

   /**
    * Post metadata that represents zero time.
    */
   public static final PostMeta ZERO = new PostMeta(0L, 0L);

   /**
    * Creates post metadata.
    * @param id The post id.
    * @param lastModifiedMillis The last modified timestamp.
    */
   public PostMeta(final long id, final long lastModifiedMillis) {
      this.id = id;
      this.lastModifiedMillis = lastModifiedMillis;
   }

   /**
    * The post id.
    */
   public final long id;

   /**
    * The last modified timestamp.
    */
   public final long lastModifiedMillis;

   @Override
   public String toString() {
      return MoreObjects.toStringHelper(this.getClass())
              .add("id", id)
              .add("lastModifiedMillis",lastModifiedMillis)
              .add("lastModifiedHumanUTC", ISODateTimeFormat.basicDateTime().withZoneUTC().print(lastModifiedMillis))
              .add("lastModifiedHuman", ISODateTimeFormat.basicDateTime().withZone(DateTimeZone.getDefault()).print(lastModifiedMillis))
              .toString();
   }

   /**
    * Converts this metadata to a byte string (for saving state).
    * @return The byte string.
    */
   public ByteString toBytes() {
      return ByteString.copyFromUtf8(id + "," + lastModifiedMillis);
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

      return new PostMeta(id, lastModifiedMillis);
   }
}
