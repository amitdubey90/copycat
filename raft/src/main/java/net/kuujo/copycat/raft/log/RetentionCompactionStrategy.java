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

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogException;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.log.LogSegment;
import net.kuujo.copycat.util.internal.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Retention based compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RetentionCompactionStrategy implements CompactionStrategy {
  private RetentionPolicy policy;

  public RetentionCompactionStrategy() {
  }

  public RetentionCompactionStrategy(RetentionPolicy policy) {
    this.policy = Assert.notNull(policy, "policy");
  }

  /**
   * Sets the retention policy.
   *
   * @param policy The retention policy.
   */
  public void setRetentionPolicy(RetentionPolicy policy) {
    this.policy = Assert.notNull(policy, "policy");
  }

  /**
   * Returns the retention policy.
   *
   * @return The retention policy.
   */
  public RetentionPolicy getRetentionPolicy() {
    return policy;
  }

  /**
   * Sets the retention policy, returning the compaction strategy for method chaining.
   *
   * @param policy The retention policy.
   * @return The retention compaction strategy.
   */
  public RetentionCompactionStrategy withRetentionPolicy(RetentionPolicy policy) {
    setRetentionPolicy(policy);
    return this;
  }

  @Override
  public RaftLog init(String name, Log log) {
    return new RetentionLog(log.getLogManager(name));
  }

  /**
   * Retention policy based Raft log.
   */
  private class RetentionLog implements RaftLog {
    private final LogManager log;
    private EntryHandler handler;

    private RetentionLog(LogManager log) {
      this.log = log;
    }

    /**
     * Checks whether the given index can be compacted and if so compacts the log.
     */
    private void checkCompact(long index) {
      Long compactIndex = null;
      for (Iterator<Map.Entry<Long, LogSegment>> iterator = log.segments().entrySet().iterator(); iterator.hasNext();) {
        Map.Entry<Long, LogSegment> entry = iterator.next();
        LogSegment segment = entry.getValue();
        if (segment.lastIndex() != null && segment.lastIndex() < index && !policy.retain(segment)) {
          compactIndex = segment.index();
        }
      }

      if (compactIndex != null) {
        try {
          log.compact(compactIndex);
        } catch (IOException e) {
          throw new LogException(e);
        }
      }
    }

    @Override
    public void open() throws IOException {
      log.open();
    }

    @Override
    public boolean isEmpty() {
      return log.isEmpty();
    }

    @Override
    public boolean isOpen() {
      return log.isOpen();
    }

    @Override
    public long size() {
      return log.size();
    }

    @Override
    public long entryCount() {
      return log.entryCount();
    }

    @Override
    public long appendEntry(RaftEntry entry) throws IOException {
      return log.appendEntry(entry.buffer());
    }

    @Override
    public long index() {
      return log.index();
    }

    @Override
    public Long firstIndex() {
      return log.firstIndex();
    }

    @Override
    public Long lastIndex() {
      return log.lastIndex();
    }

    @Override
    public boolean containsIndex(long index) {
      return log.containsIndex(index);
    }

    @Override
    public RaftEntry getEntry(long index) {
      return new RaftEntry(log.getEntry(index));
    }

    @Override
    public void removeAfter(long index) {
      log.removeAfter(index);
    }

    @Override
    public void roll(long index) throws IOException {
      log.roll(index);
    }

    @Override
    public void compact(long index) throws IOException {
      log.compact(index);
    }

    @Override
    public void flush() {
      log.flush();
    }

    @Override
    public ByteBuffer commit(long index) {
      RaftEntry entry = getEntry(index);
      ByteBuffer output = handler.apply(index, entry);
      checkCompact(index);
      return output;
    }

    @Override
    public RaftLog handler(EntryHandler handler) {
      this.handler = handler;
      return this;
    }

    @Override
    public void close() throws IOException {
      log.close();
    }

    @Override
    public boolean isClosed() {
      return log.isClosed();
    }

    @Override
    public void delete() {
      log.delete();
    }
  }

}
