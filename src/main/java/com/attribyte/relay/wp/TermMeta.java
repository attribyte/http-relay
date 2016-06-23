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

/**
 * Term metadata.
 */
public class TermMeta {

   /**
    * Creates term metadata.
    * @param id The term id.
    * @param name The term name.
    * @param slug The term slug.
    * @param groupId The id of the term group.
    */
   public TermMeta(final long id, final String name, final String slug, final long groupId) {
      this.id = id;
      this.name = name;
      this.slug = slug;
      this.groupId = groupId;
   }

   /**
    * The term id.
    */
   public final long id;

   /**
    * The term name.
    */
   public final String name;

   /**
    * The term slug.
    */
   public final String slug;

   /**
    * The id of the term group.
    */
   public final long groupId;
}
