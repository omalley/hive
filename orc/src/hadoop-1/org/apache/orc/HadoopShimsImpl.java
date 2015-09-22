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

public class HadoopShimsImpl implements HadoopShims {

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException {
    /* not supported */
    return null;
  }

  @Override
  public DirectDecompressorShim getDirectDecompressor(DirectCompressionType codec) {
    /* not supported */
    return null;
  }

  @Override
  public int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException {
    // Inefficient for direct buffers; only here for compat.
    int pos = dest.position();
    if (dest.hasArray()) {
      int result = file.read(dest.array(), dest.arrayOffset(), dest.remaining());
      if (result > 0) {
        dest.position(pos + result);
      }
      return result;
    } else {
      byte[] arr = new byte[dest.remaining()];
      int result = file.read(arr, 0, arr.length);
      if (result > 0) {
        dest.put(arr, 0, result);
        dest.position(pos + result);
      }
      return result;
    }
  }
}
