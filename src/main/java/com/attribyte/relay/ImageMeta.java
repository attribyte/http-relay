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

/**
 * Holds image metadata.
 */
public class ImageMeta {


   public ImageMeta(final int width, final int height,
                    final String sourceHash, final String contentHash) {
      this.width = width;
      this.height = height;
      this.sourceHash = sourceHash;
      this.contentHash = contentHash;
   }

   /**
    * The width.
    */
   public final int width;

   /**
    * The height.
    */
   public final int height;

   /**
    * A hash of the source URL.
    */
   public final String sourceHash;

   /**
    * A hash of the binary image.
    */
   public final String contentHash;
}
