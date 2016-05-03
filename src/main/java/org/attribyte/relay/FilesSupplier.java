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

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.Logger;
import org.attribyte.util.StringUtil;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Supplies the contents of each file in a directory.
 *
 * <h4>Configuration Properties</h4>
 *
 * <dl>
 *    <dt>sourceDir</dt>
 *    <dd>
 *       The directory that contains files/directories to be replicated.
 *    </dd>
 *
 *    <dt>recurseSourceDirs</dt>
 *    <dd>
 *       Should subdirectories of <code>sourceDir</code> be examined?
 *    </dd>
 *
 *    <dt>lostMessageDir</dt>
 *    <dd>
 *       A directory where 'lost' files are moved. Lost files can happen
 *       if there are processing or replication errors. Created as a subdirectory
 *       of the containing directory if it does not exist.
 *    </dd>
 *
 *    <dt>completedMessageDir</dt>
 *    <dd>
 *       An optional directory where, if specified, completed files
 *       are moved after replication is confirmed. Created as a subdirectory
 *       of the containing directory if it does not exist.
 *    </dd>
 *
 *    <dt>continuous</dt>
 *    <dd>
 *       If specified, and 'true', the directory will be continuously monitored for new files
 *       until shutdown.
 *    </dd>
 *
 *    <dt>saveStateInterval</dt>
 *    <dd>
 *       The number of messages to skip between state saves
 *    </dd>
 *
 *    <dt>continuousReloadPauseMillis</dt>
 *    <dd>
 *       The number of milliseconds to pause after the source directory
 *       is checked for new files.
 *    </dd>
 * </dl>
 *
 */
public class FilesSupplier implements Supplier {

   /**
    * Creates an uninitialized supplier.
    */
   public FilesSupplier() {
      this.processedSet = Sets.newConcurrentHashSet();
   }

   /**
    * Creates a files supplier.
    * @param sourceDir The source directory.
    * @param recurseSourceDirs Should subdirectories be searched?
    * @param completedMessageDir The optional (sub)directory where completed messages are moved.
    * @param lostMessageDir The (sub)directory where lost messages are moved.
    * @param continuous Does the supplier run continuously - monitoring the source directory for new files?
    * @param continuousReloadPauseMillis The number of milliseconds to pause between checks for new files when running continuously.
    * @param initialState The initial state, if any.
    * @param saveStateInterval The number of messages between state saves.
    * @param logger The logger. If <code>null</code>, messages are logged to the console.
    */
   public FilesSupplier(final File sourceDir,
                        final boolean recurseSourceDirs,
                        final Optional<String> completedMessageDir,
                        final String lostMessageDir,
                        final boolean continuous,
                        final int continuousReloadPauseMillis,
                        final Optional<ByteString> initialState,
                        final int saveStateInterval,
                        final Logger logger) throws IOException {
      this.rootDir = sourceDir;
      this.completedMessageDir = completedMessageDir;
      this.lostMessageDir = lostMessageDir;
      this.continuous = continuous;
      this.continuousReloadPauseMillis = continuousReloadPauseMillis;
      this.saveStateInterval = saveStateInterval;
      this.logger = logger != null ? logger : new ConsoleLogger();
      this.processedSet = Sets.newConcurrentHashSet();
      if(initialState.isPresent()) {
         deserializeState(initialState.get());
      }
      this.processedSetFilter = pathname -> pathname.isFile() && pathname.canRead() && !processedSet.contains(pathname.getAbsolutePath());
      this.recurseSourceDirs = recurseSourceDirs;
      if(recurseSourceDirs) {
         recurseSourceDirs();
      } else {
         sourceDirs = ImmutableList.of(rootDir).iterator();
      }
      createIterator();
      state = State.MESSAGE;
   }

   @Override
   public Message nextMessage() {

      if(state == State.STOPPED || !isInit.get()) {
         return Message.stop();
      }

      if(iter.hasNext()) {
         if(state == State.MESSAGE) {
            final File curr = iter.next();
            final String id = curr.getAbsolutePath();
            try {
               final byte[] fileBytes = Files.toByteArray(curr);
               messageSize.update(fileBytes.length);
               final long completedMessageCount = completedMessages.getCount();
               if(saveStateInterval == 0) {
                  state = State.SAVE_STATE;
               } else if(completedMessageCount % saveStateInterval == 0) {
                  logger.info(String.format("Saving state (%s messages completed)", completedMessageCount));
                  state = State.SAVE_STATE;
               }

               final Timer.Context ctx = publishedMessages.time();
               try {
                  if(fileBytes.length > 0) {
                     return Message.publish(id, Files.toByteArray(curr));
                  } else {
                     logger.info(String.format("Skipping zero-byte message for %s", id));
                     completedMessage(Message.publish(id, ByteString.EMPTY));
                     return Message.pause(0);
                  }
               } finally {
                  ctx.stop();
               }
            } catch(Error e) {
               logger.error("Fatal error", e);
               throw e;
            } catch(Throwable t) {
               logger.error("Problem loading message", t);
               loadErrors.inc();
               state = State.MESSAGE;
               lostMessage(Message.publish(id, ByteString.EMPTY));
               return Message.error(String.format("Problem loading message from '%s'", curr.getAbsolutePath()));
            }
         } else if(state == State.SAVE_STATE) {
            state = State.MESSAGE;
            return Message.state(serializeState());
         } else {
            return Message.stop();
         }
      } else if(!continuous) {
         if(!sourceDirs.hasNext()) {
            if(isComplete()) {
               state = State.STOPPED;
               return Message.state(serializeState());
            } else {
               if(state == State.SAVE_STATE) {
                  state = State.FINISHING;
                  return Message.state(serializeState());
               } else {
                  state = State.SAVE_STATE;
                  return Message.pause(continuousReloadPauseMillis);
               }
            }
         } else {
            reloads.inc();
            createIterator();
            state = State.MESSAGE;
            return Message.pause(continuousReloadPauseMillis);
         }
      } else {
         reloads.inc();
         if(recurseSourceDirs && !sourceDirs.hasNext()) {
            try {
               recurseSourceDirs();
            } catch(IOException ioe) {
               filesystemErrors.inc();
               logger.error("Problem checking directory", ioe);
               return Message.pause(continuousReloadPauseMillis);
            }
         }
         createIterator();
         state = State.MESSAGE;
         return Message.pause(continuousReloadPauseMillis);
      }
   }

   @Override
   public void completedMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      processedSet.add(message.id);
      if(completedMessageDir.isPresent()) {
         moveMessage(message, completedMessageDir.get(), "Problem moving completed message from '%s' to '%s'");
      }
      completedMessages.mark();
   }

   @Override
   public void lostMessage(Message message) {
      timeToAcknowledge.update(System.currentTimeMillis() - message.createTimeMillis, TimeUnit.MILLISECONDS);
      processedSet.add(message.id);
      moveMessage(message, lostMessageDir, "Problem moving lost message from '%s' to '%s'");
      lostMessages.inc();
   }

   /**
    * Moves a message to a subdirectory.
    * @param message The message.
    * @param subdirectory The subdirectory.
    * @param errorFormat The error format string to use.
    * @return Was the message moved?
    */
   private boolean moveMessage(final Message message,
                               final String subdirectory,
                               final String errorFormat) {
      File source = new File(message.id);
      File sourceDir = source.getParentFile();
      File destDir = new File(sourceDir, subdirectory);
      if(!destDir.exists()) {
         if(!destDir.mkdir()) {
            filesystemErrors.inc();
            logger.error("Unable to create: " + destDir.getAbsolutePath());
            return false;
         }
      }
      File dest = new File(destDir, source.getName());
      try {
         Files.move(source, dest);
      } catch(IOException ioe) {
         filesystemErrors.inc();
         logger.error(String.format(errorFormat, source.getAbsolutePath(), dest.getAbsolutePath()), ioe);
         return false;
      }
      return true;
   }

   @Override
   public void init(final Properties props,
                    final Optional<ByteString> savedState,
                    final Logger logger) throws Exception {
      if(isInit.compareAndSet(false, true)) {
         rootDir = new File(props.getProperty("sourceDir", "."));
         this.logger = logger;

         if(!rootDir.exists()) {
            throw new Exception(String.format("The directory, '%s' (%s), does not exist", "sourceDir", rootDir.getAbsolutePath()));
         }

         if(!rootDir.isDirectory()) {
            throw new Exception(String.format("The directory, '%s' (%s), is a file", "sourceDir", rootDir.getAbsolutePath()));
         }

         if(!rootDir.canRead()) {
            throw new Exception(String.format("The directory, '%s' (%s), is not readable", "sourceDir", rootDir.getAbsolutePath()));
         }

         lostMessageDir = props.getProperty("lostMessageDir", "lost").trim();

         String completedMessageDir = props.getProperty("completedMessageDir", "").trim();
         this.completedMessageDir = completedMessageDir.isEmpty() ? Optional.absent() : Optional.of(completedMessageDir);

         continuous = props.getProperty("continuous", "false").equalsIgnoreCase("true");
         saveStateInterval = StringUtil.parseInt(props.getProperty("saveStateInterval"), 0);

         continuousReloadPauseMillis = StringUtil.parseInt(props.getProperty("continuousReloadPauseMillis"), DEFAULT_CONTINUOUS_RELOAD_PAUSE);
         if(savedState.isPresent()) {
            deserializeState(savedState.get());
         }
         processedSetFilter = pathname -> pathname.isFile() && pathname.canRead() && !processedSet.contains(pathname.getAbsolutePath());

         recurseSourceDirs = StringUtil.parseBoolean(props.getProperty("recurseSourceDirs"), false);

         if(recurseSourceDirs) {
            recurseSourceDirs();
         } else {
            sourceDirs = ImmutableList.of(rootDir).iterator();
         }
         createIterator();
         state = State.MESSAGE;
      }
   }

   @Override
   public Optional<ByteString> shutdown() {
      if(isInit.compareAndSet(true, false)) {
         return Optional.of(serializeState());
      } else {
         return Optional.absent();
      }
   }

   /**
    * Creates the iterator over the files.
    */
   private void createIterator() {
      if(sourceDirs.hasNext()) {
         File sourceDir = sourceDirs.next();
         processedSet.add(sourceDir.getName());
         File[] files = sourceDir.listFiles(processedSetFilter);
         List<File> fileList = files != null ? ImmutableList.copyOf(files) : ImmutableList.of();
         iter = fileList.iterator();
      } else {
         iter = ImmutableList.<File>of().iterator();
      }
   }

   /**
    * Serializes the internal state (processed set) one filename per line.
    * @return The state.
    */
   private ByteString serializeState() {
      return ByteString.copyFromUtf8(Joiner.on('\n').skipNulls().join(processedSet));
   }

   /**
    * Deserializes a saved state.
    * @param state The saved state.
    */
   private void deserializeState(final ByteString state) {
      Splitter.on('\n')
              .trimResults()
              .omitEmptyStrings()
              .split(state.toStringUtf8())
              .forEach(processedSet::add);
   }

   /**
    * Starting at <code>sourceDir</code>, find all subdirectories
    * and add them to the process directory iteration.
    * Excludes symbolic links.
    * @throws IOException on read error.
    */
   private void recurseSourceDirs() throws IOException {
      final List<File> processDirs = Lists.newArrayListWithExpectedSize(64);
      Path start = rootDir.toPath();
      java.nio.file.Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
         @Override
         public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
         }
         @Override
         public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            File file = dir.toFile();
            if(file.equals(rootDir)) {
               return FileVisitResult.CONTINUE;
            } else if(file.getName().equals(Strings.nullToEmpty(lostMessageDir))) {
               return FileVisitResult.CONTINUE;
            } else if(completedMessageDir.isPresent() && file.getName().equals(completedMessageDir.get())) {
               return FileVisitResult.CONTINUE;
            } else {
               processDirs.add(file);
               return FileVisitResult.CONTINUE;
            }
         }
      });
      Collections.sort(processDirs);
      this.sourceDirs = processDirs.iterator();
   }

   /**
    * Determine if all published messages have been reported either completed or failed.
    * @return The complete status.
    */
   private boolean isComplete() {
      return publishedMessages.getCount() - completedMessages.getCount() - lostMessages.getCount() == 0;
   }

   /**
    * The root directory that contains all files/directories to process.
    */
   private File rootDir;

   /**
    * Should the source directories be searched?
    */
   private boolean recurseSourceDirs;

   /**
    * An iterator over all source directories.
    */
   private Iterator<File> sourceDirs;

   /**
    * The optional subdirectory to which completed files are moved.
    */
   private Optional<String> completedMessageDir = Optional.absent();

   /**
    * The subdirectory where lost message files are moved.
    */
   private String lostMessageDir;

   /**
    * The iterator over the files currently in the source directory.
    */
   private Iterator<File> iter;

   /**
    * In continuous mode, the iterator will be refreshed (maybe new files are continuously added).
    */
   private boolean continuous;

   /**
    * The amount of time in milliseconds to pause after the iterator is reloaded.
    */
   private int continuousReloadPauseMillis;

   /**
    * Saves the state every N process messages.
    */
   private int saveStateInterval = 0;

   /**
    * The default reload pause (5 seconds).
    */
   private static final int DEFAULT_CONTINUOUS_RELOAD_PAUSE = 5000;

   /**
    * Is the supplier initialized?
    */
   private final AtomicBoolean isInit = new AtomicBoolean(false);

   /**
    * The logger.
    */
   private Logger logger;

   /**
    * A set of files already processed.
    */
   private final Set<String> processedSet;

   /**
    * A filter that excludes already processed files.
    */
   private FileFilter processedSetFilter;

   /**
    * Times published messages.
    */
   private final Timer publishedMessages = new Timer();

   /**
    * Records message size.
    */
   private final Histogram messageSize = new Histogram(new ExponentiallyDecayingReservoir());

   /**
    * Counts message load errors.
    */
   private final Counter loadErrors = new Counter();

   /**
    * Counts any errors related to the filesystem.
    */
   private final Counter filesystemErrors = new Counter();

   /**
    * Counts the number of continuous reloads.
    */
   private final Counter reloads = new Counter();

   /**
    * Counts the number of lost messages.
    */
   private final Counter lostMessages = new Counter();

   /**
    * Counts the number of lost messages.
    */
   private final Meter completedMessages = new Meter();

   /**
    * The time elapsed between message create and the (async) response from the target.
    */
   private final Timer timeToAcknowledge = new Timer();

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("completed-messages", completedMessages);
      builder.put("message-size", messageSize);
      builder.put("time-to-acknowledge", timeToAcknowledge);
      builder.put("filesystem-errors", filesystemErrors);
      builder.put("load-errors", publishedMessages);
      builder.put("replication-pauses", loadErrors);
      builder.put("reloads", reloads);
      builder.put("lost-messages", lostMessages);
      return builder.build();
   }

   /**
    * Internal state.
    */
   private enum State {

      /**
       * Send a message next.
       */
      MESSAGE,

      /**
       * Pause next.
       */
      SAVE_STATE,

      /**
       * State is stopped.
       */
      STOPPED,

      /**
       * State is finishing.
       */
      FINISHING
   }

   /**
    * The current internal state. Must only be changed in the <code>nextMessage</code> method.
    */
   private State state = State.STOPPED;
}