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

package org.attribyte.relay;

import org.apache.log4j.PropertyConfigurator;
import org.attribyte.api.Logger;

import java.io.IOException;

/**
 * The command line interface.
 */
public class CLI extends org.attribyte.util.CLI {

   /**
    * Creates the command line interface.
    * @param args The command line arguments.
    */
   public CLI(final String appName, String args[]) throws IOException {
      super(appName, DEFAULT_INSTALL_DIR_PROP, args);
   }

   @Override
   protected Logger initLogger() {
      PropertyConfigurator.configure(logProps);
      final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(appName);
      return new Logger() {
         public void debug(final String s) { logger.debug(s); }
         public void info(final String s) { logger.info(s); }
         public void warn(final String s) { logger.warn(s); }
         public void warn(final String s, final Throwable throwable) { logger.warn(s, throwable); }
         public void error(final String s) { logger.error(s); }
         public void error(final String s, final Throwable throwable) { logger.error(s, throwable); }
      };
   }
}