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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TestOutStream {

  @Test
  public void testUncompressedEncryptedStream() throws Exception {
    byte[] key = new byte[16];
    byte[] iv = new byte[16];
    for(int i=0; i < 16; ++i) {
      key[i] = (byte) i;
      iv[i] = (byte) (10 * i);
    }
    Cipher cipher = Cipher.Factory.get(Cipher.Algorithm.AES128_CTR);
    cipher.initialize(Cipher.Mode.ENCRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    byte[] plain = "In a sufficiently large cluster there are no corner cases."
        .getBytes();
    TestInStream.OutputCollector receiver = new TestInStream.OutputCollector();
    OutStream out = new OutStream("test", 10, null, receiver, cipher);
    out.write(plain);
    out.flush();
    assertEquals(plain.length, receiver.buffer.size());
    assertArrayEquals(new byte[]{-83, -29, -127, 19, 80, 83, -115, 59, 78, -119,
        -6, 91, -116, -93, -50, 51, 4, 74, -100, 113, 36, 118, 28, 120, -127,
        -42, 107, 75, 113, 21, -40, -37, -117, -70, 86, -33, 5, 27, -11, -22,
        -13, -94, 113, 127, -115, -102, 6, 37, 37, 102, 74, -113, 50, -119, 59,
        12, -80, -120}, receiver.buffer.get());
  }

  @Test
  public void testCompressedEncryptedStream() throws Exception {
    byte[] key = new byte[16];
    byte[] iv = new byte[16];
    for(int i=0; i < 16; ++i) {
      key[i] = (byte) i;
      iv[i] = (byte) (10 * i);
    }
    Cipher cipher = Cipher.Factory.get(Cipher.Algorithm.AES128_CTR);
    cipher.initialize(Cipher.Mode.ENCRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    byte[] plain = ("In pioneer days they used oxen for heavy pulling, and" +
        " when one ox couldn't budge a log, they didn't try to" +
        " grow a larger ox. We shouldn't be trying for bigger" +
        " computers, but for more systems of computers.").getBytes();
    TestInStream.OutputCollector receiver = new TestInStream.OutputCollector();
    OutStream out = new OutStream("test", 64, new ZlibCodec(), receiver,
        cipher);
    out.write(plain);
    out.flush();
    byte[] expected = new byte[]{
      122, 0, 0,
        -23, 70, 122, 123, -16, 16, -12, 88, -8, -75, 119, 50, -57, 47, -94,
        -34, 33, 39, 81, 52, -13, 30, 76, -125, 57, 85, -35, 1, 24, -35, 46,
        104, -30, -8, 10, -99, -19, -78, -95, 48, 30, 13, -111, 59, -105, 117,
        -101, 115, -111, -87, -115, 87, 44, -88, 20, -104, 25, 16, 116, -123,
        -1,
      120, 0, 0,
        -128, -36, 39, 59, 9, -114, -13, -12, 73, -18, 111, 73, -78, 14, 64,
        -108, -1, -48, -87, 63, -92, 77, -39, -18, 56, 82, 10, 44, 63, -47, -62,
        -126, -26, 80, 97, -72, -35, -57, 117, 57, -34, -108, 98, 28, 31, -71,
        62, -118, 73, 94, -95, -73, -126, 103, -40, -36, 102, -99, -1, 107,
      118, 0, 0,
        -116, 83, -100, 0, -111, -88, -119, 86, 23, -60, -90, 74, 23, -7, 123,
        -94, -115, 115, 44, 90, 90, -76, 83, 119, 105, -46, -128, -25, 112, -70,
        118, -94, 28, 97, 50, -56, 127, 116, -118, -90, -113, 8, 28, 91, 13,
        -73, 104, 109, 64, 31, 79, -34, 16, -84, -9, 41, -44, -28, 73,
      25, 0, 0,
        85, -35, -20, -61, -2, 84, -96, -117, 109, 79, 13, 68
    };
    assertEquals(expected.length, receiver.buffer.size());
    byte[] output = receiver.buffer.get();
    assertArrayEquals(expected, receiver.buffer.get());
  }

}
