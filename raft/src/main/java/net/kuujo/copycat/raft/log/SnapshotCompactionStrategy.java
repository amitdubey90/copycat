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

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.log.LogSegment;
import net.kuujo.copycat.util.internal.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Snapshot compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotCompactionStrategy implements CompactionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotCompactionStrategy.class);
  private SnapshotReader reader;
  private SnapshotWriter writer;

  public SnapshotCompactionStrategy() {
  }

  public SnapshotCompactionStrategy(SnapshotReader reader, SnapshotWriter writer) {
    this.reader = Assert.notNull(reader, "reader");
    this.writer = Assert.notNull(writer, "writer");
  }

  /**
   * Sets the snapshot reader.
   *
   * @param reader The snapshot reader.
   */
  public void setReader(SnapshotReader reader) {
    this.reader = Assert.notNull(reader, "reader");
  }

  /**
   * Returns the snapshot reader.
   *
   * @return The snapshot reader.
   */
  public SnapshotReader getReader() {
    return reader;
  }

  /**
   * Sets the snapshot reader, returning the compaction strategy for method chaining.
   *
   * @param reader The snapshot reader.
   * @return The snapshot compaction strategy.
   */
  public SnapshotCompactionStrategy withReader(SnapshotReader reader) {
    setReader(reader);
    return this;
  }

  /**
   * Sets the snapshot writer.
   *
   * @param writer The snapshot writer.
   */
  public void setWriter(SnapshotWriter writer) {
    this.writer = Assert.notNull(writer, "writer");
  }

  /**
   * Returns the snapshot writer.
   *
   * @return The snapshot writer.
   */
  public SnapshotWriter getWriter() {
    return writer;
  }

  /**
   * Sets the snapshot writer, returning the configuration for method chaining.
   *
   * @param writer The snapshot writer.
   * @return The snapshot compaction strategy.
   */
  public SnapshotCompactionStrategy withWriter(SnapshotWriter writer) {
    setWriter(writer);
    return this;
  }

  @Override
  public RaftLog init(String name, Log log) {
    return new SnapshotLog(name, log.getLogManager(name), log.getLogManager(String.format("%s.snapshot", name)));
  }

  /**
   * Snapshot log implementation.
   */
  private class SnapshotLog implements RaftLog {
    private static final int SNAPSHOT_CHUNK_SIZE = 1024 * 1024;
    private static final int SNAPSHOT_INFO = 0;
    private static final int SNAPSHOT_CHUNK = 1;
    private final String name;
    private final LogManager logManager;
    private final LogManager snapshotManager;
    private EntryHandler handler;
    private SnapshotInfo snapshotInfo;
    private List<ByteBuffer> snapshotChunks;

    public SnapshotLog(String name, LogManager logManager, LogManager snapshotManager) {
      this.name = name;
      this.logManager = logManager;
      this.snapshotManager = snapshotManager;
    }

    @Override
    public void open() throws IOException {
      snapshotManager.open();
      logManager.open();
    }

    @Override
    public boolean isEmpty() {
      return snapshotManager.isEmpty() && logManager.isEmpty();
    }

    @Override
    public boolean isOpen() {
      return snapshotManager.isOpen() && logManager.isOpen();
    }

    @Override
    public long size() {
      return snapshotManager.size() + logManager.size();
    }

    @Override
    public long entryCount() {
      return snapshotManager.entryCount() + logManager.entryCount();
    }

    @Override
    public ByteBuffer commit(long index) {
      RaftEntry entry = getEntry(index);
      ByteBuffer output = null;
      if (entry.type() == RaftEntry.Type.SNAPSHOT) {
        installSnapshot(entry);
      } else {
        output = handler.apply(index, entry);
        if (isSnapshottable(index)) {
          snapshot(index);
        }
      }
      return output;
    }

    @Override
    public RaftLog handler(EntryHandler handler) {
      this.handler = handler;
      return this;
    }

    /**
     * Returns a boolean value indicating whether the given index is a snapshottable index.
     *
     * @param index The index to check.
     * @return Indicates whether a snapshot can be taken at the given index.
     */
    private boolean isSnapshottable(long index) {
      if (!logManager.containsIndex(index)) {
        return false;
      }
      LogSegment segment = logManager.segment(index);
      if (segment == null) {
        return false;
      } else if (segment.lastIndex() == null || segment.lastIndex() != index) {
        return false;
      } else if (segment == logManager.lastSegment()) {
        return false;
      }
      return true;
    }

    /**
     * Takes a snapshot at the given index.
     */
    private void snapshot(long index) {
      RaftEntry entry = getEntry(index);
      long term = entry.term();

      String id = UUID.randomUUID().toString();
      LOGGER.info("{} - Taking snapshot {}", name, id);

      ByteBuffer snapshotBuffer = writer != null ? writer.writeSnapshot() : ByteBuffer.allocate(0);

      // Create a unique snapshot ID and calculate the number of chunks for the snapshot.
      LOGGER.debug("{} - Calculating snapshot chunk size for snapshot {}", name, id);
      byte[] snapshotId = id.getBytes();
      int numChunks = (int) Math.ceil(snapshotBuffer.limit() / (double) SNAPSHOT_CHUNK_SIZE);

      LOGGER.debug("{} - Creating {} chunks for snapshot {}", name, numChunks, id);
      List<RaftEntry> chunks = new ArrayList<>(numChunks + 1);

      // The first entry in the snapshot is snapshot metadata.
      ByteBuffer info = ByteBuffer.allocate(16 + snapshotId.length);
      info.putInt(SNAPSHOT_INFO);
      info.putInt(snapshotId.length);
      info.put(snapshotId);
      info.putInt(snapshotBuffer.limit());
      info.putInt(numChunks);
      chunks.add(new RaftEntry(RaftEntry.Type.SNAPSHOT, term, info));

      // Now we append a list of snapshot chunks. This ensures that snapshots can be easily replicated in chunks.
      int i = 0;
      int position = 0;
      while (position < snapshotBuffer.limit()) {
        byte[] bytes = new byte[Math.min(snapshotBuffer.limit() - position, SNAPSHOT_CHUNK_SIZE)];
        snapshotBuffer.get(bytes);
        ByteBuffer chunk = ByteBuffer.allocate(12 + bytes.length);
        chunk.putInt(SNAPSHOT_CHUNK);
        chunk.putInt(i++); // The position of the chunk in the snapshot.
        chunk.putInt(bytes.length); // The snapshot chunk length.
        chunk.put(bytes); // The snapshot chunk bytes.
        chunk.flip();
        chunks.add(new RaftEntry(RaftEntry.Type.SNAPSHOT, term, chunk));
        position += bytes.length;
      }

      try {
        LOGGER.debug("{} - Appending {} chunks for snapshot {} at index {}", name, chunks.size(), id, index);
        appendSnapshot(index, chunks);
      } catch (IOException e) {
        throw new CopycatException("Failed to compact state log", e);
      }
    }

    /**
     * Appends a snapshot to the log.
     *
     * @param index The index at which to write the snapshot.
     * @param snapshot The snapshot to append to the snapshot log.
     * @return The index at which the snapshot was written.
     * @throws java.io.IOException If the log could not be rolled over.
     */
    private long appendSnapshot(long index, List<RaftEntry> snapshot) throws IOException {
      LogSegment segment = logManager.segment(index);
      if (segment == null) {
        throw new IndexOutOfBoundsException("Invalid snapshot index " + index);
      } else if (segment.lastIndex() != index) {
        throw new IllegalArgumentException("Snapshot index must be the last index of a segment");
      } else if (segment == logManager.lastSegment()) {
        throw new IllegalArgumentException("Cannot snapshot current log segment");
      }

      // When appending a snapshot, force the snapshot log manager to roll over to a new segment, append the snapshot
      // to the log, and then compact the log once the snapshot has been appended.
      snapshotManager.roll(index - snapshot.size() + 1);
      for (RaftEntry entry : snapshot) {
        snapshotManager.appendEntry(entry.buffer());
      }

      // Compact the snapshot and user logs in order to ensure old entries do not remain in the logs.
      snapshotManager.compact(index - snapshot.size() + 1);
      logManager.compact(index + 1);
      return index;
    }

    /**
     * Installs a snapshot.
     *
     * This method operates on the assumption that snapshots will always be replicated with an initial metadata entry.
     * The metadata entry specifies a set of chunks that complete the entire snapshot.
     */
    @SuppressWarnings("unchecked")
    private void installSnapshot(RaftEntry snapshotChunk) {
      // Get the snapshot entry type.
      int type = snapshotChunk.entry().getInt();
      if (type == SNAPSHOT_INFO) {
        // The snapshot info defines the snapshot ID and number of chunks. When a snapshot info entry is processed,
        // reset the current snapshot chunks and store the snapshot info. The next entries to be applied should be the
        // snapshot chunks themselves.
        int idLength = snapshotChunk.entry().getInt();
        byte[] idBytes = new byte[idLength];
        snapshotChunk.entry().get(idBytes);
        String id = new String(idBytes);
        int size = snapshotChunk.entry().getInt();
        int numChunks = snapshotChunk.entry().getInt();
        if (snapshotInfo == null || !snapshotInfo.id.equals(id)) {
          LOGGER.debug("{} - Processing snapshot metadata for snapshot {}", name, id);
          snapshotInfo = new SnapshotInfo(id, size, numChunks);
          snapshotChunks = new ArrayList<>(numChunks);
        }
      } else if (type == SNAPSHOT_CHUNK && snapshotInfo != null) {
        // When a chunk is received, use the chunk's position in the snapshot to ensure consistency. Extract the chunk
        // bytes and only append the the chunk if it matches the expected position in the local chunks list.
        int index = snapshotChunk.entry().getInt();
        int chunkLength = snapshotChunk.entry().getInt();
        byte[] chunkBytes = new byte[chunkLength];
        snapshotChunk.entry().get(chunkBytes);
        if (snapshotChunks.size() == index) {
          LOGGER.debug("{} - Processing snapshot chunk {} for snapshot {}", name, index, snapshotInfo.id);
          snapshotChunks.add(ByteBuffer.wrap(chunkBytes));

          // Once the number of chunks has grown to the complete expected chunk count, combine and install the snapshot.
          if (snapshotChunks.size() == snapshotInfo.chunks) {
            LOGGER.debug("{} - Completed assembly of snapshot {} from log", name, snapshotInfo.id);
            if (reader != null) {
              // Calculate the total aggregated size of the snapshot.
              int size = 0;
              for (ByteBuffer chunk : snapshotChunks) {
                size += chunk.limit();
              }

              // Make sure the combined snapshot size is equal to the expected snapshot size.
              Assert.state(size == snapshotInfo.size, "Received inconsistent snapshot");

              LOGGER.debug("{} - Assembled snapshot size: {} bytes", name, size);

              if (size > 0) {
                // Create a complete view of the snapshot by appending all chunks to each other.
                ByteBuffer completeSnapshot = ByteBuffer.allocate(size);
                snapshotChunks.forEach(completeSnapshot::put);
                completeSnapshot.flip();

                // Once a view of the snapshot has been created, deserialize and install the snapshot.
                LOGGER.info("{} - Installing snapshot {}", name, snapshotInfo.id);

                try {
                  reader.readSnapshot(completeSnapshot);
                } catch (Exception e) {
                  LOGGER.warn("{} - Failed to install snapshot: {}", name, e.getMessage());
                }
              }
            }
            snapshotInfo = null;
            snapshotChunks = null;
          }
        }
      }
    }

    @Override
    public long appendEntry(RaftEntry entry) throws IOException {
      return logManager.appendEntry(entry.buffer());
    }

    @Override
    public long index() {
      return snapshotManager.index();
    }

    @Override
    public Long firstIndex() {
      return !snapshotManager.isEmpty() ? snapshotManager.firstIndex() : logManager.firstIndex();
    }

    @Override
    public Long lastIndex() {
      return logManager.lastIndex();
    }

    @Override
    public boolean containsIndex(long index) {
      Assert.state(isOpen(), "Log is not open");
      return logManager.containsIndex(index) || snapshotManager.containsIndex(index);
    }

    @Override
    public RaftEntry getEntry(long index) {
      Assert.state(isOpen(), "Log is not open");
      if (logManager.containsIndex(index)) {
        return new RaftEntry(logManager.getEntry(index));
      } else if (snapshotManager.containsIndex(index)) {
        return new RaftEntry(snapshotManager.getEntry(index));
      }
      throw new IndexOutOfBoundsException("No entry at index " + index);
    }

    @Override
    public void removeAfter(long index) {
      Assert.state(isOpen(), "Log is not open");
      Assert.index(index, logManager.containsIndex(index), "Log index out of bounds");
      logManager.removeAfter(index);
    }

    @Override
    public void roll(long index) throws IOException {
      logManager.roll(index);
      snapshotManager.close();
      snapshotManager.delete();
      snapshotManager.open();
    }

    @Override
    public void compact(long index) throws IOException {
      snapshotManager.compact(index);
      logManager.compact(index);
    }

    @Override
    public void flush() {
      logManager.flush();
    }

    @Override
    public void close() throws IOException {
      logManager.close();
      snapshotManager.close();
    }

    @Override
    public boolean isClosed() {
      return logManager.isClosed();
    }

    @Override
    public void delete() {
      logManager.delete();
      snapshotManager.delete();
    }

    /**
     * Current snapshot info.
     */
    private class SnapshotInfo {
      private final String id;
      private final int size;
      private final int chunks;

      private SnapshotInfo(String id, int size, int chunks) {
        this.id = id;
        this.size = size;
        this.chunks = chunks;
      }
    }

    /**
     * Snapshot chunk.
     */
    private class SnapshotChunk {
      private final int index;
      private final ByteBuffer chunk;

      private SnapshotChunk(int index, ByteBuffer chunk) {
        this.index = index;
        this.chunk = chunk;
      }
    }

  }

}
