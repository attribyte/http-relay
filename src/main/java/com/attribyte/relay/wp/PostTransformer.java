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


import com.attribyte.client.ClientProtos;
import org.attribyte.wp.model.Post;

import java.util.Properties;

/**
 * Transform/transfer fields from a post to an entry (builder).
 */
public interface PostTransformer {

   /**
    * Transform a post to an entry (builder).
    * @param post The post.
    * @param entry The entry builder.
    */
   public void transform(final Post post, ClientProtos.WireMessage.Entry.Builder entry);

   /**
    * Initialize the transformer.
    * @param props The configuration properties.
    * @throws Exception on initialization error.
    */
   public void init(final Properties props) throws Exception;
}