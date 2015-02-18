/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.log;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftLog {

  /**
   * Registers an apply handler on the log.
   *
   * @return The Raft log.
   */
  RaftLog handler(EntryHandler handler);

  /**
   * Returns the current log commit index.
   *
   * @return The current log commit index.
   */
  Long commitIndex();

  /**
   * Returns the last entry applied to the state machine.
   *
   * @return The last entry applied to the state machine.
   */
  Long lastApplied();

  /**
   * Opens the logger.
   */
  void open() throws IOException;

  /**
   * Returns a boolean indicating whether the logger is empty.
   *
   * @return Indicates whether the logger is empty.
   */
  boolean isEmpty();

  /**
   * Returns a boolean indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  boolean isOpen();

  /**
   * Returns the size in bytes.
   *
   * @return The size in bytes.
   * @throws IllegalStateException If the log is not open.
   */
  long size();

  /**
   * Returns the number of entries.
   *
   * @return The number of entries.
   * @throws IllegalStateException If the log is not open.
   */
  long entryCount();

  /**
   * Appends an entry to the logger.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws IllegalStateException If the log is not open.
   * @throws NullPointerException If the entry is null.
   * @throws java.io.IOException If a new segment cannot be opened
   */
  long appendEntry(RaftEntry entry) throws IOException;

  /**
   * Returns the immutable first index in the log.
   *
   * @return The first index in the log.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  long index();

  /**
   * Returns the index of the first entry in the log.
   *
   * @return The index of the first entry in the log or {@code null} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  Long firstIndex();

  /**
   * Returns the index of the last entry in the log.
   *
   * @return The index of the last entry in the log or {@code null} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  Long lastIndex();

  /**
   * Returns a boolean indicating whether the log contains an entry at the given index.
   *
   * @param index The index of the entry to check.
   * @return Indicates whether the log contains the given index.
   * @throws IllegalStateException If the log is not open.
   */
  boolean containsIndex(long index);

  /**
   * Gets an entry from the log.
   *
   * @param index The index of the entry to get.
   * @return The entry at the given index, or {@code null} if the entry doesn't exist.
   * @throws IllegalStateException If the log is not open.
   */
  RaftEntry getEntry(long index);

  /**
   * Removes all entries after the given index (exclusive).
   *
   * @param index The index after which to remove entries.
   * @throws IllegalStateException If the log is not open.
   * @throws net.kuujo.copycat.log.LogException If a new segment cannot be opened
   */
  void removeAfter(long index);

  /**
   * Forces the log to roll over to a new segment.
   *
   * @throws IOException If the log failed to create a new segment.
   */
  void roll(long index) throws IOException;

  /**
   * Flushes the log to disk.
   *
   * @throws IllegalStateException If the log is not open.
   */
  void flush();

  /**
   * Commits the given index to the log.
   *
   * @param index The index to commit.
   */
  ByteBuffer commit(long index);

  /**
   * Compacts the log, removing all segments up to and including the given index.
   *
   * @param index The index to which to compact the log. This must be the first index of the last
   *          segment in the log to remove via compaction
   * @throws IllegalArgumentException if {@code index} is not the first index of a segment or if
   *           {@code index} represents the last segment in the log
   * @throws IndexOutOfBoundsException if {@code index} is out of bounds for the log
   * @throws IOException If the log failed to delete a segment.
   */
  void compact(long index) throws IOException;

  /**
   * Closes the logger.
   */
  void close() throws IOException;

  /**
   * Returns a boolean indicating whether the log is closed.
   *
   * @return Indicates whether the log is closed.
   */
  boolean isClosed();

  /**
   * Deletes the logger.
   */
  void delete();

}
