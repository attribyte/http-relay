/*
 * Copyright 2017 Attribyte, LLC
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.attribyte.wp.model.Shortcode;
import org.attribyte.wp.model.ShortcodeParser;

import java.text.ParseException;
import java.util.Properties;

/**
 * A content transformer that replaces "self-closing" shortcodes with a single space
 * and replaces "enclosing" shortcodes with their content, if any.
 */
public class ShortcodeCleaner implements ContentTransformer {

   @Override
   public String transform(final String content) {
      IgnoreHandler handler = new IgnoreHandler();
      try {
         Shortcode.parse(content, handler);
         return handler.buf.toString();
      } catch(Exception pe) { //Don't die for any reason...just return the original content.
         return content;
      }
   }

   /**
    * Removes shortcodes from content, replacing them with a single space.
    */
   private static class IgnoreHandler implements ShortcodeParser.Handler {

      @Override
      public void shortcode(Shortcode shortcode) {
         if(!Strings.isNullOrEmpty(shortcode.content)) {
            switch(shortcode.name) {
               case "embed":
                  buf.append("<embed src=\"").append(shortcode.content).append("\"> ");
                  break;
               default:
                  buf.append(shortcode.content).append(" ");
                  break;
            }
         } else {
            buf.append(" ");
         }
      }

      @Override
      public void text(String text) {
         buf.append(text);
      }

      @Override
      public void parseError(String text, ParseException pe) {
         //Ignore...
      }

      @Override
      public Shortcode.Type type(final String shortcode) {
         return standardShortcodes.getOrDefault(shortcode, Shortcode.Type.SELF_CLOSING);
      }

      private final StringBuilder buf = new StringBuilder();
   }

   /**
    * A map of types for standard shortcodes.
    */
   private static final ImmutableMap<String, Shortcode.Type> standardShortcodes =
           ImmutableMap.<String, Shortcode.Type>builder()
           .put("audio", Shortcode.Type.SELF_CLOSING)
           .put("caption", Shortcode.Type.ENCLOSING)
           .put("embed", Shortcode.Type.ENCLOSING)
           .put("gallery", Shortcode.Type.SELF_CLOSING)
           .put("playlist", Shortcode.Type.SELF_CLOSING)
           .put("video", Shortcode.Type.SELF_CLOSING)
           .build();

   @Override
   public void init(final Properties props) throws Exception {
      //Do nothing...
   }

}
