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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertArrayEquals;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.DiskRange;
import org.junit.Test;

public class TestInStream {

  static class OutputCollector implements OutStream.OutputReceiver {
    DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      this.buffer.add(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }
  }

  static class PositionCollector
      implements PositionProvider, PositionRecorder {
    private List<Long> positions = new ArrayList<Long>();
    private int index = 0;

    @Override
    public long getNext() {
      return positions.get(index++);
    }

    @Override
    public void addPosition(long offset) {
      positions.add(offset);
    }

    public void reset() {
      index = 0;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("position: ");
      for(int i=0; i < positions.size(); ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(positions.get(i));
      }
      return builder.toString();
    }
  }

  @Test
  public void testUncompressed() throws Exception {
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", 100, null, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals(1024, collect.buffer.size());
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, collect.buffer.get(i));
    }
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", new ByteBuffer[]{inBuf},
        new long[]{0}, inBuf.remaining(), null, 100, null);
    assertEquals("uncompressed stream test position: 0 length: 1024" +
                 " range: 0 offset: 0 limit: 0",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCompressed() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 300, codec, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    assertEquals(961, collect.buffer.size());
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", new ByteBuffer[]{inBuf},
        new long[]{0}, inBuf.remaining(), codec, 300, null);
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                 " offset: 0 limit: 0 range 0 = 0 to 961",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCorruptStream() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 500, codec, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();

    // now try to read the stream with a buffer that is too small
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", new ByteBuffer[]{inBuf},
        new long[]{0}, inBuf.remaining(), codec, 100, null);
    byte[] contents = new byte[1024];
    try {
      in.read(contents);
      fail();
    } catch(IllegalArgumentException iae) {
      // EXPECTED
    }

    // make a corrupted header
    inBuf.clear();
    inBuf.put((byte) 32);
    inBuf.put((byte) 0);
    inBuf.flip();
    in = InStream.create("test2", new ByteBuffer[]{inBuf}, new long[]{0},
        inBuf.remaining(), codec, 300, null);
    try {
      in.read();
      fail();
    } catch (IllegalStateException ise) {
      // EXPECTED
    }
  }

  @Test
  public void testDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 400, codec, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    DataOutput stream = new DataOutputStream(out);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      stream.writeInt(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    assertEquals(1674, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(500);
    inBuf[1] = ByteBuffer.allocate(1200);
    inBuf[2] = ByteBuffer.allocate(500);
    collect.buffer.setByteBuffer(inBuf[0], 0, 483);
    collect.buffer.setByteBuffer(inBuf[1], 483, 1625 - 483);
    collect.buffer.setByteBuffer(inBuf[2], 1625, 1674 - 1625);

    for(int i=0; i < inBuf.length; ++i) {
      inBuf[i].flip();
    }
    InStream in = InStream.create("test", inBuf,
        new long[]{0,483, 1625}, 1674, codec, 400, null);
    assertEquals("compressed stream test position: 0 length: 1674 range: 0" +
                 " offset: 0 limit: 0 range 0 = 0 to 483;" +
                 "  range 1 = 483 to 1142;  range 2 = 1625 to 49",
                 in.toString());
    DataInputStream inStream = new DataInputStream(in);
    for(int i=0; i < 1024; ++i) {
      int x = inStream.readInt();
      assertEquals(i, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i, inStream.readInt());
    }

    in = InStream.create("test", new ByteBuffer[]{inBuf[1], inBuf[2]},
        new long[]{483, 1625}, 1674, codec, 400, null);
    inStream = new DataInputStream(in);
    positions[303].reset();
    in.seek(positions[303]);
    for(int i=303; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }

    in = InStream.create("test", new ByteBuffer[]{inBuf[0], inBuf[2]},
        new long[]{0, 1625}, 1674, codec, 400, null);
    inStream = new DataInputStream(in);
    positions[1001].reset();
    for(int i=0; i < 300; ++i) {
      assertEquals(i, inStream.readInt());
    }
    in.seek(positions[1001]);
    for(int i=1001; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }
  }

  @Test
  public void testUncompressedDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", 400, null, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    DataOutput stream = new DataOutputStream(out);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      stream.writeInt(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    assertEquals(4096, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(1100);
    inBuf[1] = ByteBuffer.allocate(2200);
    inBuf[2] = ByteBuffer.allocate(1100);
    collect.buffer.setByteBuffer(inBuf[0], 0, 1024);
    collect.buffer.setByteBuffer(inBuf[1], 1024, 2048);
    collect.buffer.setByteBuffer(inBuf[2], 3072, 1024);

    for(int i=0; i < inBuf.length; ++i) {
      inBuf[i].flip();
    }
    InStream in = InStream.create("test", inBuf,
        new long[]{0, 1024, 3072}, 4096, null, 400, null);
    assertEquals("uncompressed stream test position: 0 length: 4096" +
                 " range: 0 offset: 0 limit: 0",
                 in.toString());
    DataInputStream inStream = new DataInputStream(in);
    for(int i=0; i < 1024; ++i) {
      int x = inStream.readInt();
      assertEquals(i, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i, inStream.readInt());
    }

    in = InStream.create("test", new ByteBuffer[]{inBuf[1], inBuf[2]},
        new long[]{1024, 3072}, 4096, null, 400, null);
    inStream = new DataInputStream(in);
    positions[256].reset();
    in.seek(positions[256]);
    for(int i=256; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }

    in = InStream.create("test", new ByteBuffer[]{inBuf[0], inBuf[2]},
        new long[]{0, 3072}, 4096, null, 400, null);
    inStream = new DataInputStream(in);
    positions[768].reset();
    for(int i=0; i < 256; ++i) {
      assertEquals(i, inStream.readInt());
    }
    in.seek(positions[768]);
    for(int i=768; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }
  }

  @Test
  public void testUncompressedEncryptedStream() throws Exception {
    byte[] key = new byte[16];
    byte[] iv = new byte[16];
    for(int i=0; i < 16; ++i) {
      key[i] = (byte) i;
      iv[i] = (byte) (10 * i);
    }
    Cipher cipher = Cipher.Factory.get(Cipher.Algorithm.AES128_CTR);
    cipher.initialize(Cipher.Mode.DECRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    byte[] encrypted = new byte[]{-76, -24, -50, 2, 28, 69, -40, 57, 71, -64,
        -22, 70, -100, -67, -45, 59, 93, 25, -124, 101, 48, 119, 89, 57, -114,
        -42, 62, 87, 99, 80, -34, -109, -102, -14, 71, -60, 13, 94, -70, -72,
        -34, -29, 123, 127, -62, -119, 73, 59, 46, 119, 75, -113, 37, -128, 45,
        4, -29, -62, -90, -75, -77, 95, -7, 31, -100, 66, 99, -55, 73, 111, -89,
        88, 33, -7, 55, 41, -88, -55, -7, -111, -110, 67};
    List<DiskRange> ranges = new ArrayList<>();
    ranges.add(new RecordReaderImpl.BufferChunk(ByteBuffer.wrap(encrypted), 0));
    InStream in = InStream.create("test", ranges, encrypted.length, null,
        10000, cipher);
    byte[] decrypt = new byte[6];
    in.read(decrypt);
    assertArrayEquals("People".getBytes(), decrypt);
    assertEquals(' ', in.read());
    decrypt = new byte[encrypted.length - 7];
    in.read(decrypt);
    assertArrayEquals(("do stupid stuff all of the time." +
        " Hadoop lets them do stupid stuff at scale.").getBytes(), decrypt);
    PositionCollector posn = new PositionCollector();
    posn.addPosition(0);
    in.seek(posn);
    decrypt = new byte[encrypted.length];
    in.read(decrypt);
    assertArrayEquals(("People do stupid stuff all of the time." +
        " Hadoop lets them do stupid stuff at scale.").getBytes(), decrypt);
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
    cipher.initialize(Cipher.Mode.DECRYPT, ByteBuffer.wrap(key),
        ByteBuffer.wrap(iv), 0);
    byte[] encrypted = new byte[]{96, 0, 0, 23, -60, -19, -68, 38, -24, -73, 14,
        96, 41, -75, 120, -92, -29, 115, -109, -78, -71, -95, -40, -99, 62, 40,
        -112, 99, -80, -119, 92, -29, -42, 64, -45, -101, -60, 24, 57, 4, -97,
        -66, -116, -126, 103, 56, -11, 9, 31, 107, 87};
    List<DiskRange> ranges = new ArrayList<>();
    ranges.add(new RecordReaderImpl.BufferChunk(ByteBuffer.wrap(encrypted), 0));
    InStream in = InStream.create("test", ranges, encrypted.length,
        new ZlibCodec(), 10000, cipher);
    byte[] decrypt = new byte[4];
    in.read(decrypt);
    assertArrayEquals("Lack".getBytes(), decrypt);
    assertEquals(' ', in.read());
    decrypt = new byte[encrypted.length - 5];
    in.read(decrypt);
    assertArrayEquals(("of direction, not lack of time, is the problem"
                       ).getBytes(), decrypt);
    PositionCollector posn = new PositionCollector();
    posn.addPosition(0);
    posn.addPosition(0);
    in.seek(posn);
    decrypt = new byte[encrypted.length];
    in.read(decrypt);
    assertArrayEquals(("Lack of direction, not lack of time, is the problem"
                       ).getBytes(), decrypt);
  }
}
