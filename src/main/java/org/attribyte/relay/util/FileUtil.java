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

import com.google.common.io.Files;

public class FileUtil {

   /**
    * Gets the extension for a file or URL.
    * <p>
    *    Includes the '{@code .}'.
    * </p>
    * @param url The URL.
    * @return The extension or an empty string if none found.
    */
   public static final String getExtension(final String url) {
      int end = url.indexOf('?');
      if(end < 0) {
         end = url.indexOf('#');
      }
      final String ext;
      if(end > 0) {
         ext = Files.getFileExtension(url.substring(0, end));
      } else {
         ext = Files.getFileExtension(url);
      }
      return ext.isEmpty() ? "" : "." + ext;
   }
}
