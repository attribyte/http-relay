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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.attribyte.relay;

import com.attribyte.client.ClientProtos;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class HTMLUtil {

   /**
    * Extracts links (to citations) and images.
    * @param entry The entry.
    * @param baseURI The URI used to resolve relative links.
    */
   public static final void extractLinks(final ClientProtos.WireMessage.Entry.Builder entry, final String baseURI) {
      Document doc = Jsoup.parse(entry.getContent(), baseURI);
      Elements links = doc.select("a[href]");
      for(Element link : links) {
         String href = Strings.nullToEmpty(link.attr("href")).trim();
         if(!href.isEmpty()) {
            entry.addCitationsBuilder()
                    .setDirection(ClientProtos.WireMessage.Citation.Direction.OUT)
                    .setLink(href);
         }
      }

      Elements images = doc.select("img[src]");
      for(Element image : images) {
         String src = Strings.nullToEmpty(image.attr("src")).trim();
         if(!src.isEmpty()) {
            ClientProtos.WireMessage.Image.Builder imageBuilder = entry.addImagesBuilder().setOriginalSrc(src);
            String alt = Strings.nullToEmpty(image.attr("alt")).trim();
            if(!alt.isEmpty()) {
               imageBuilder.setAltText(alt);
            }

            String title = Strings.nullToEmpty(image.attr("title")).trim();
            if(!title.isEmpty()) {
               imageBuilder.setTitle(title);
            }

            String heightStr = Strings.nullToEmpty(image.attr("height")).trim();
            String widthStr = Strings.nullToEmpty(image.attr("width")).trim();
            if(!heightStr.isEmpty() && !widthStr.isEmpty()) {
               Integer height = Ints.tryParse(heightStr);
               Integer width = Ints.tryParse(widthStr);
               if(height != null && width != null) {
                  imageBuilder.setHeight(height).setWidth(width);
               }
            }
         }
      }

      if(entry.getImagesCount() > 0) { //Set the primary image as the first image.
         entry.setPrimaryImage(entry.getImages(0));
      }
   }
}
