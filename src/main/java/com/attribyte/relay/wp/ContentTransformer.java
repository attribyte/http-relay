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


import java.util.Properties;

/**
 * Transform content (markup cleaning, shortcode processing, etc).
 */
public interface ContentTransformer {

   /**
    * Transform the content.
    * @param content The content to transform.
    * @return The transformed content.
    */
   public String transform(final String content);


   /**
    * Initialize the transformer.
    * @param props The configuration properties.
    * @throws Exception on initialization error.
    */
   public void init(final Properties props) throws Exception;
}