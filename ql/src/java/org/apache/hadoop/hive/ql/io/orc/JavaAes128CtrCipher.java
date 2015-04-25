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
package org.apache.hadoop.hive.ql.io.orc;

import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * This class implements AES/CTR using the Java encryption library.
 */
class JavaAes128CtrCipher implements Cipher {

  private static int BLOCK_LENGTH = 16;
  private Mode mode;
  private long offset;
  private javax.crypto.Cipher cipher;
  private SecretKeySpec key;
  private long[] baseIv = new long[2];
  private ByteBuffer offsetIv = ByteBuffer.allocate(BLOCK_LENGTH);

  public JavaAes128CtrCipher() {
    try {
      cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Unknown algorithm", e);
    } catch (NoSuchPaddingException e) {
      throw new IllegalArgumentException("Unknown padding", e);
    }
  }

  @Override
  public int getKeyLength() {
    return BLOCK_LENGTH;
  }

  @Override
  public int getIvLength() {
    return BLOCK_LENGTH;
  }

  @Override
  public void initialize(Mode _mode, ByteBuffer _key, ByteBuffer _iv,
                         long _offset) {
    mode = _mode;
    offset = _offset;
    key = new SecretKeySpec(_key.array(),
        _key.arrayOffset() + _key.arrayOffset(), BLOCK_LENGTH, "AES");
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_LENGTH);
    _iv.rewind();
    buffer.put(_iv);
    _iv.rewind();
    buffer.flip();
    LongBuffer longBuffer = buffer.asLongBuffer();
    longBuffer.get(baseIv);
    updateIv(offset / BLOCK_LENGTH);
    IvParameterSpec iv = new IvParameterSpec(offsetIv.array(),
        offsetIv.arrayOffset(), BLOCK_LENGTH);
    try {
      cipher.init(mode.getJavaMode(), key, iv);
    } catch (InvalidKeyException ike) {
      throw new IllegalArgumentException("Bad parameters to init", ike);
    } catch (InvalidAlgorithmParameterException iape) {
      throw new IllegalArgumentException("bad parameters to init", iape);
    }
    skipPartBlock((int)(offset % 16));
  }

  void updateIv(long blockId) {
    offsetIv.clear();
    LongBuffer longBuffer = offsetIv.asLongBuffer();
    long low = baseIv[1] + blockId;
    long high = baseIv[0];
    if (low < baseIv[1]) {
      high += 1;
    }
    longBuffer.put(high);
    longBuffer.put(low);
    offsetIv.limit(BLOCK_LENGTH);
    offsetIv.rewind();
  }

  void skipPartBlock(int bytes) {
    if (bytes != 0) {
      byte[] trash = new byte[bytes];
      try {
        cipher.update(trash, 0, bytes, trash);
      } catch (ShortBufferException e) {
        throw new IllegalArgumentException("short buffer", e);
      }
    }
  }
  @Override
  public void seek(long position) {
    if (position != offset) {
      offset = position;
      updateIv(offset / BLOCK_LENGTH);
      IvParameterSpec iv = new IvParameterSpec(offsetIv.array(),
          offsetIv.arrayOffset(), BLOCK_LENGTH);
      try {
        cipher.init(mode.getJavaMode(), key, iv);
      } catch (InvalidKeyException ike) {
        throw new IllegalArgumentException("Bad parameters to init", ike);
      } catch (InvalidAlgorithmParameterException iape) {
        throw new IllegalArgumentException("bad parameters to init", iape);
      }
      skipPartBlock((int) (offset % BLOCK_LENGTH));
    }
  }

  @Override
  public void update(ByteBuffer input, ByteBuffer output) {
    int consumed = Math.min(input.remaining(), output.remaining());
    offset += consumed;
    try {
      int outputPosition = output.position();
      int inputPosition = input.position();
      int len = cipher.update(input.array(),
                              input.arrayOffset() + inputPosition,
                              consumed, output.array(),
                              output.arrayOffset() + outputPosition);
      input.position(len + inputPosition);
      output.position(outputPosition + len);
    } catch (ShortBufferException sbe) {
      throw new IllegalArgumentException("buffer problem", sbe);
    }
  }
}
