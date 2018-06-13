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

package com.attribyte.relay.wp;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;

public class PostMetaDB {

   /**
    * Creates the DB.
    * @param dir The directory for files.
    */
   public PostMetaDB(final File dir) {
      this.dir = dir;
   }

   /**
    * Writes post meta.
    * @param meta The meta to write.
    * @return The input metadata.
    * @throws IOException on write error.
    */
   public final PostMeta write(final PostMeta meta) throws IOException {
      File file = file(meta.id);
      file.getParentFile().mkdirs();
      Files.write(meta.toBytes().toByteArray(), file);
      return meta;
   }

   /**
    * Reads post meta for an id.
    * @param id The id.
    * @return The post meta or {@code null} if not found.
    * @throws IOException on read error.
    */
   public final PostMeta read(final long id) throws IOException {
      File file = file(id);
      if(file.exists() && file.canRead()) {
         return PostMeta.fromBytes(ByteString.copyFrom(Files.toByteArray(file)));
      } else {
         return null;
      }
   }

   /**
    * Creates the file for storage with a subdirectory based on
    * the last two digits of the id.
    * @param id The id.
    * @return The file.
    */
   public final File file(final long id) {
      String idStr = Long.toString(id);
      final String subdir;
      if(idStr.length() > 1) {
         subdir = idStr.substring(idStr.length() - 2);
      } else {
         subdir = "0" + idStr;
      }
      return new File(new File(dir, subdir), idStr);
   }

   /**
    * The directory for files.
    */
   private final File dir;
}
