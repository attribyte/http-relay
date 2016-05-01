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

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

/**
 * A relay message.
 */
public class Message {

   public enum Type {

      /**
       * Message is to be published.
       */
      PUBLISH,

      /**
       * Message indicates a pause in replication.
       */
      PAUSE,

      /**
       * Message indicates replication should stop.
       */
      STOP,

      /**
       * Message indicates an error.
       */
      ERROR,

      /**
       * Message contains internal state to be saved.
       */
      STATE
   }

   /**
    * Creates a publish message from a byte string.
    * @param id The message id.
    * @param message The message.
    * @return The message.
    */
   public static Message publish(final String id, final ByteString message) {
      return new Message(id, message, Type.PUBLISH);
   }

   /**
    * Creates a publish message from a byte array.
    * @param id The message id.
    * @param message The message.
    * @return The message.
    */
   public static Message publish(final String id, final byte[] message) {
      return new Message(id, ByteString.copyFrom(message), Type.PUBLISH);
   }

   /**
    * Creates a save state message.
    * @param state The state to save.
    * @return The message.
    */
   public static Message state(final ByteString state) {
      return new Message("", state, Type.STATE);
   }

   /**
    * Creates a pause.
    * @param milliseconds The number of milliseconds to pause.
    * @return The message.
    */
   public static Message pause(final int milliseconds) {
      return new PauseMessage(milliseconds);
   }

   /**
    * Creates a stop message.
    * @return The message.
    */
   public static Message stop() {
      return new Message("", (ByteString)null, Type.STOP);
   }

   /**
    * Creates an error message.
    * @param message An error message.
    * @return The message.
    */
   public static Message error(final String message) {
      return new Message("", message, Type.ERROR);
   }

   /**
    * Creates a pause message.
    */
   private static class PauseMessage extends Message {
      PauseMessage(final int value) {
         super("", Integer.toString(value), Type.PAUSE);
      }

      @Override
      public Integer toInt() {
         return message != null ? Ints.tryParse(message.toStringUtf8()) : null;
      }
   }

   /**
    * Creates a publish message from a string.
    * @param id The message id.
    * @param message The message.
    * @param type The message type.
    */
   private Message(final String id, final String message, final Type type) {
      this(id, ByteString.copyFromUtf8(message), type);
   }

   /**
    * Creates a publish message from bytes.
    * @param id The message id.
    * @param message The message.
    * @param type The message type.
    */
   private Message(final String id, final ByteString message, final Type type) {
      this.id = Strings.nullToEmpty(id);
      this.message = message;
      this.type = type;
      this.attempts = 0;
      this.createTimeMillis = System.currentTimeMillis();
   }

   /**
    * Creates a publish message from a byte string.
    * @param id The message id.
    * @param message The message.
    */
   private Message(final String id, final ByteString message,
                   final int attempts, final long createTimeMillis) {
      this.id = Strings.nullToEmpty(id);
      this.message = message;
      this.type = Type.PUBLISH;
      this.attempts = attempts;
      this.createTimeMillis = createTimeMillis;
   }

   /**
    * The message id. Not null.
    */
   public final String id;

   /**
    * The message as bytes. May be null.
    */
   public final ByteString message;

   /**
    * The message type.
    */
   public final Type type;

   /**
    * The number of attempts for this message.
    */
   public final int attempts;

   /**
    * The time the message was created.
    */
   public final long createTimeMillis;

   /**
    * Is this a state message?
    * @return Is this a state message?
    */
   public boolean isState() {
      return type == Type.STATE;
   }

   /**
    * Is this a publish message?
    * @return Is this a publish message?
    */
   public boolean isPublish() {
      return type == Type.PUBLISH;
   }

   /**
    * Is this a pause message?
    * @return Is this a pause message?
    */
   public boolean isPause() {
      return type == Type.PAUSE;
   }

   /**
    * Is this an error message?
    * @return Is this an error message?
    */
   public boolean isError() {
      return type == Type.ERROR;
   }

   /**
    * Gets the message as an integer, if possible.
    * @return The message as an integer or <code>null</code> if not an int.
    */
   public Integer toInt() {
      return null;
   }


   @Override
   public String toString() {
      String messageStr = message != null ? message.toStringUtf8() : null;
      return MoreObjects.toStringHelper(this)
              .add("id", id)
              .add("message", messageStr)
              .add("type", type)
              .add("attempts", attempts)
              .add("createTimeMillis", createTimeMillis)
              .toString();
   }

   /**
    * Return a copy of this message with the number of attempts incremented.
    * @return The new message.
    */
   public Message incrementAttempts() {
      return new Message(this.id, this.message, this.attempts + 1, this.createTimeMillis);
   }

   /**
    * Computes the next exponential back-off (delay) time for this message
    * based on the number of attempts.
    * @param delayIntervalMillis The base delay interval in milliseconds.
    * @return The back-off time.
    */
   public final long nextBackOffTime(long delayIntervalMillis) {
      return (long)(Math.pow(2, attempts) * delayIntervalMillis);
   }
}
