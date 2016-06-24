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

package org.attribyte.relay.util;

import com.attribyte.client.ClientProtos;

import java.net.UnknownHostException;
import java.util.TimeZone;

public class MessageUtil {

   /**
    * The hostname.
    */
   public static final String HOSTNAME = getHostname();

   /**
    * Gets the hostname.
    * @return The hostname.
    */
   private static String getHostname() {
      try {
         return java.net.InetAddress.getLocalHost().getHostName();
      } catch(UnknownHostException ue) {
         return "[unknown]";
      }
   }

   /**
    * Builds the server origin for messages.
    * @return The server origin.
    */
   public static final ClientProtos.WireMessage.Origin buildServerOrigin() {
      return buildServerOrigin(null, null);
   }

   /**
    * Builds the server origin for messages.
    * @param imageBaseURL The base URL for images. May be {@code null}.
    * @param iconBaseURL The base URL for icons. May be {@code null}.
    * @return The origin.
    */
   public static final ClientProtos.WireMessage.Origin buildServerOrigin(String imageBaseURL, String iconBaseURL) {
      ClientProtos.WireMessage.Origin.Builder builder = ClientProtos.WireMessage.Origin.newBuilder();
      builder.setCurrentTimestamp(System.currentTimeMillis());
      builder.setServerId(HOSTNAME);
      builder.setTimezone(TimeZone.getDefault().getID());
      if(imageBaseURL != null) {
         builder.setImageBaseURL(imageBaseURL);
      }

      if(iconBaseURL != null) {
         builder.setIconBaseURL(iconBaseURL);
      }

      return builder.build();
   }
}
