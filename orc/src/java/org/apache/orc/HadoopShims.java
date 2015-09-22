/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface HadoopShims {

  /**
   * a hadoop.io ByteBufferPool shim.
   */
  public interface ByteBufferPoolShim {
    /**
     * Get a new ByteBuffer from the pool.  The pool can provide this from
     * removing a buffer from its internal cache, or by allocating a
     * new buffer.
     *
     * @param direct     Whether the buffer should be direct.
     * @param length     The minimum length the buffer will have.
     * @return           A new ByteBuffer. Its capacity can be less
     *                   than what was requested, but must be at
     *                   least 1 byte.
     */
    ByteBuffer getBuffer(boolean direct, int length);

    /**
     * Release a buffer back to the pool.
     * The pool may choose to put this buffer into its cache/free it.
     *
     * @param buffer    a direct bytebuffer
     */
    void putBuffer(ByteBuffer buffer);
  }

  /**
   * Reads data into ByteBuffer.
   * @param file File.
   * @param dest Buffer.
   * @return Number of bytes read, just like file.read. If any bytes were read, dest position
   *         will be set to old position + number of bytes read.
   */
  int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException;

  /**
   * Provides an HDFS ZeroCopyReader shim.
   * @param in FSDataInputStream to read from (where the cached/mmap buffers are tied to)
   * @param in ByteBufferPoolShim to allocate fallback buffers with
   *
   * @return returns null if not supported
   */
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException;

  public interface ZeroCopyReaderShim {
    /**
     * Get a ByteBuffer from the FSDataInputStream - this can be either a HeapByteBuffer or an MappedByteBuffer.
     * Also move the in stream by that amount. The data read can be small than maxLength.
     *
     * @return ByteBuffer read from the stream,
     */
    public ByteBuffer readBuffer(int maxLength, boolean verifyChecksums) throws IOException;
    /**
     * Release a ByteBuffer obtained from a read on the
     * Also move the in stream by that amount. The data read can be small than maxLength.
     *
     */
    public void releaseBuffer(ByteBuffer buffer);
  }

  public enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  };

  public interface DirectDecompressorShim {
    public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException;
  }

  public DirectDecompressorShim getDirectDecompressor(DirectCompressionType codec);

  public static class Factory {
    private static final HadoopShims SHIMS;
    static {
      try {
        Class<HadoopShims> cls = (Class<HadoopShims>)
            Class.forName("org.apache.orc.HadoopShimsImpl");
        SHIMS = cls.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Initialization problem", e);
      } catch (InstantiationException e) {
        throw new IllegalStateException("Initialization problem", e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Initialization problem", e);
      }
    }

    public static HadoopShims get() {
      return SHIMS;
    }
  }
}
