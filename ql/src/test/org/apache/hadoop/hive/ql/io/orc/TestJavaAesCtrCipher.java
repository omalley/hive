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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test the Java AES/CTR implementation.
 */
public class TestJavaAesCtrCipher {

  @Test
  public void TestEncryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.ENCRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    ByteBuffer input = ByteBuffer.allocate(64);
    for(int i=0; i < 64; ++i) {
      input.put((byte) i);
    }
    input.flip();
    ByteBuffer output = ByteBuffer.allocate(64);
    cipher.update(input, output);
    assertArrayEquals(new byte[]{-0x1b, -0xc, 0x21, -0x43, 0x5b, 0x69, 0x25,
        -0x7f, 0x2a, -0x2d, -0x22, -0x28, 0x13, -0x42, -0x7f, -0x7c, 0x35, 0x7a,
        0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21, -0x43, 0x64, 0x26, 0x4c,
        -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49, -0x56, 0x69, 0x6a, -0x54, 0x7b,
        -0x57, 0x27, -0x51, -0x7b, 0x61, 0xd, 0x4, -0x64, 0x34, -0x34, -0x1b,
        -0x51, -0x4, -0x26, 0x4e, 0x24, 0x44, 0x15, 0x1c, 0x10, -0x10, 0x28,
        0x18}, output.array());
  }

  @Test
  public void TestMultipartEncryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.ENCRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    ByteBuffer input = ByteBuffer.allocate(64);
    for(int i=0; i < 64; ++i) {
      input.put((byte) i);
    }
    input.flip();
    ByteBuffer output = ByteBuffer.allocate(64);
    input.limit(20);
    cipher.update(input, output);
    assertEquals(20, output.position());
    input.limit(40);
    cipher.update(input, output);
    assertEquals(40, output.position());
    input.limit(64);
    cipher.update(input, output);
    assertEquals(64, output.position());
    assertArrayEquals(new byte[]{-0x1b, -0xc, 0x21, -0x43, 0x5b, 0x69, 0x25,
        -0x7f, 0x2a, -0x2d, -0x22, -0x28, 0x13, -0x42, -0x7f, -0x7c, 0x35, 0x7a,
        0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21, -0x43, 0x64, 0x26, 0x4c,
        -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49, -0x56, 0x69, 0x6a, -0x54, 0x7b,
        -0x57, 0x27, -0x51, -0x7b, 0x61, 0xd, 0x4, -0x64, 0x34, -0x34, -0x1b,
        -0x51, -0x4, -0x26, 0x4e, 0x24, 0x44, 0x15, 0x1c, 0x10, -0x10, 0x28,
        0x18}, output.array());
  }

  @Test
  public void TestInPlaceEncryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.ENCRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    ByteBuffer input = ByteBuffer.allocate(64);
    for(int i=0; i < 64; ++i) {
      input.put((byte) i);
    }
    input.flip();
    input.limit(20);
    cipher.update(input, input);
    assertEquals(20, input.position());
    input.limit(40);
    cipher.update(input, input);
    assertEquals(40, input.position());
    input.limit(64);
    cipher.update(input, input);
    assertEquals(64, input.position());
    assertArrayEquals(new byte[]{-0x1b, -0xc, 0x21, -0x43, 0x5b, 0x69, 0x25,
        -0x7f, 0x2a, -0x2d, -0x22, -0x28, 0x13, -0x42, -0x7f, -0x7c, 0x35, 0x7a,
        0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21, -0x43, 0x64, 0x26, 0x4c,
        -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49, -0x56, 0x69, 0x6a, -0x54, 0x7b,
        -0x57, 0x27, -0x51, -0x7b, 0x61, 0xd, 0x4, -0x64, 0x34, -0x34, -0x1b,
        -0x51, -0x4, -0x26, 0x4e, 0x24, 0x44, 0x15, 0x1c, 0x10, -0x10, 0x28,
        0x18}, input.array());
  }

  @Test
  public void TestDecryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.DECRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    ByteBuffer input =
        ByteBuffer.wrap(new byte[]{-0x1b, -0xc, 0x21, -0x43, 0x5b,
        0x69, 0x25, -0x7f, 0x2a, -0x2d, -0x22, -0x28, 0x13, -0x42, -0x7f, -0x7c,
        0x35, 0x7a, 0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21, -0x43, 0x64,
        0x26, 0x4c, -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49, -0x56, 0x69, 0x6a,
        -0x54, 0x7b, -0x57, 0x27, -0x51, -0x7b, 0x61, 0xd, 0x4, -0x64, 0x34,
        -0x34, -0x1b, -0x51, -0x4, -0x26, 0x4e, 0x24, 0x44, 0x15, 0x1c, 0x10,
        -0x10, 0x28, 0x18});
    ByteBuffer output = ByteBuffer.allocate(64);
    cipher.update(input, output);
    byte[] expected = new byte[64];
    for(int i=0; i < 64; ++i) {
      expected[i] = (byte) i;
    }
    assertArrayEquals(expected, output.array());
  }

  @Test
  public void TestEvenOffsetDecryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.DECRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 16);
    ByteBuffer input =
        ByteBuffer.wrap(new byte[]{
            0x35, 0x7a, 0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21, -0x43,
            0x64, 0x26, 0x4c, -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49, -0x56, 0x69,
            0x6a, -0x54, 0x7b, -0x57, 0x27, -0x51, -0x7b, 0x61, 0xd, 0x4, -0x64,
            0x34, -0x34, -0x1b, -0x51, -0x4, -0x26, 0x4e, 0x24, 0x44, 0x15,
            0x1c, 0x10, -0x10, 0x28, 0x18});
    ByteBuffer output = ByteBuffer.allocate(48);
    cipher.update(input, output);
    byte[] expected = new byte[48];
    for(int i=0; i < 48; ++i) {
      expected[i] = (byte) (i + 16);
    }
    assertArrayEquals(expected, output.array());
  }

  @Test
  public void TestSkipDecryption() throws Exception {
    JavaAesCtrCipher cipher = new JavaAesCtrCipher();
    byte[] key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    byte[] iv = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32};
    cipher.initialize(Cipher.Mode.DECRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    ByteBuffer input =
        ByteBuffer.wrap(new byte[]{-0x1b, -0xc, 0x21, -0x43, 0x5b,
            0x69, 0x25, -0x7f, 0x2a, -0x2d, -0x22, -0x28, 0x13, -0x42, -0x7f,
            -0x7c, 0x35, 0x7a, 0x28, 0x40, -0x5e, 0x11, 0x6, 0x68, 0x6d, 0x21,
            -0x43, 0x64, 0x26, 0x4c, -0x40, 0x40, 0x23, -0x63, 0x4f, 0x49,
            -0x56, 0x69, 0x6a, -0x54, 0x7b, -0x57, 0x27, -0x51, -0x7b, 0x61,
            0xd, 0x4, -0x64, 0x34, -0x34, -0x1b, -0x51, -0x4, -0x26, 0x4e, 0x24,
            0x44, 0x15, 0x1c, 0x10, -0x10, 0x28, 0x18});
    input.limit(20);
    ByteBuffer output = ByteBuffer.allocate(20);
    cipher.update(input, output);
    assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        14, 15, 16, 17, 18, 19}, output.array());
    cipher.seek(50);
    input.limit(64);
    input.position(50);
    output.clear();
    cipher.update(input, output);
    assertEquals(14, output.position());
    assertArrayEquals(new byte[]{50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
        62, 63, 14, 15, 16, 17, 18, 19}, output.array());
    cipher.seek(10);
    input.limit(30);
    input.position(10);
    output.clear();
    cipher.update(input, output);
    assertEquals(20, output.position());
    assertArrayEquals(new byte[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 23, 24, 25, 26, 27, 28, 29}, output.array());
  }
}
