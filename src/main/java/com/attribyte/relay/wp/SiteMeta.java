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
import org.joda.time.DateTime;

/**
 * Site metadata.
 */
public class SiteMeta {

   public SiteMeta(final long id,
                   final String baseURL, final String title,
                   final String description,
                   final String permalinkStructure,
                   final TermMeta defaultCategory) {
      this.id = id;
      this.baseURL = baseURL;
      this.title = title;
      this.description = description;
      this.permalinkStructure = permalinkStructure;
      this.defaultCategory = defaultCategory;
   }

   /**
    * The site id.
    */
   public final long id;

   /**
    * The base (home) URL for the site.
    */
   public final String baseURL;

   /**
    * The site title.
    */
   public final String title;

   /**
    * The site description.
    */
   public final String description;

   /**
    * The permalink format string.
    */
   public final String permalinkStructure;

   /**
    * The default category.
    */
   public final TermMeta defaultCategory;


   /*
      %year%
         The year of the post, four digits, for example 2004
      %monthnum%
         Month of the year, for example 05
      %day%
         Day of the month, for example 28
      %hour%
         Hour of the day, for example 15
      %minute%
         Minute of the hour, for example 43
      %second%
         Second of the minute, for example 33
      %post_id%
         The unique ID # of the post, for example 423
      %postname%
         A sanitized version of the title of the post (post slug field on Edit Post/Page panel). So “This Is A Great Post!” becomes this-is-a-great-post in the URI.
      %category%
         A sanitized version of the category name (category slug field on New/Edit Category panel). Nested sub-categories appear as nested directories in the URI.
      %author%
         A sanitized version of the author name.
    */

   /**
    * Builds the permalink for an entry.
    * @param entry The entry (or entry builder).
    * @return The permalink string.
    * @see <a href="https://codex.wordpress.org/Using_Permalinks">https://codex.wordpress.org/Using_Permalinks</a>
    */
   public String buildPermalink(final ClientProtos.WireMessage.EntryOrBuilder entry,
                                final String postSlug, final String authorSlug) {

      final String post_id = entry.getUID().getId();
      final DateTime publishTime = new DateTime(entry.getPublishTimeMillis());
      final String year = Integer.toString(publishTime.getYear());
      final String monthnum = String.format("%2d", publishTime.getMonthOfYear());
      final String day = String.format("%2d", publishTime.getDayOfMonth());
      final String hour = String.format("%2d", publishTime.getHourOfDay());
      final String minute = String.format("%2d", publishTime.getMinuteOfHour());
      final String second = String.format("%2d", publishTime.getSecondOfMinute());
      final String path = permalinkStructure
              .replace("%year%", year)
              .replace("%monthnum%", monthnum)
              .replace("%day%", day)
              .replace("%hour%", hour)
              .replace("%minute%", minute)
              .replace("%second%", second)
              .replace("%post_id%", post_id)
              .replace("%postname%", postSlug)
              .replace("%author%", authorSlug);
      return baseURL + path;
   }

   protected String getPermalinkCategory(final ClientProtos.WireMessage.EntryOrBuilder entry) {
      return null;
   }
}
