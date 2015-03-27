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

import java.nio.ByteBuffer;

public interface Cipher {
  public enum Mode {ENCRYPT(javax.crypto.Cipher.ENCRYPT_MODE),
    DECRYPT(javax.crypto.Cipher.DECRYPT_MODE);

    private int code;

    private Mode(int _code) {
      code = _code;
    }

    public int getJavaMode() {
      return code;
    }
  }

  /**
   * Get the size of the key.
   * @return the number of bytes in the key.
   */
  public int getKeyLength();

  /**
   * Get the size of the iv.
   * @return the number of bytes in the iv
   */
  public int getIvLength();

  /**
   * Initialize this object to operate in this mode.
   * @param mode whether to encrypt or decrypt
   * @param key the key value
   * @param iv the iv value
   * @param offset the byte offset to start at
   */
  public void initialize(Mode mode, ByteBuffer key, ByteBuffer iv, long offset);

  /**
   * For decryption, seek to the given position.
   * @param bytes
   */
  public void seek(long bytes);

  /**
   * Process another set of bytes.
   * @param input the bytes to encrypt or decrypt
   * @param output the output bytes, which should be the same size
   */
  public void update(ByteBuffer input, ByteBuffer output);

  public enum Algorithm {AES_CTR}

  public class Factory {
    public static Cipher get(Algorithm algorithm) {
      return new JavaAesCtrCipher();
    }
  }
}
