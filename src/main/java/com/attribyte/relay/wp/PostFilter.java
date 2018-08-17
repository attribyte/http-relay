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


import org.attribyte.wp.model.Post;

import java.util.List;
import java.util.Properties;

/**
 * Filter a list of posts.
 */
public interface PostFilter {

   /**
    * Filter a list of posts.
    * @param posts The posts.
    * @return The filtered post list.
    */
   public List<Post> filter(final List<Post> posts);

   /**
    * Initialize the filter.
    * @param props The configuration properties.
    * @throws Exception on initialization error.
    */
   public void init(final Properties props) throws Exception;
}