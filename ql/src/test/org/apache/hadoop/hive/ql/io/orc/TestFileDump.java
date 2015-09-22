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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveTestUtils;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.StripeStatistics;
import org.junit.Before;
import org.junit.Test;

public class TestFileDump {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestFileDump.testDump.orc");
    fs.delete(testFilePath, false);
  }

  static class MyRecord {
    int i;
    long l;
    String s;
    MyRecord(int i, long l, String s) {
      this.i = i;
      this.l = l;
      this.s = s;
    }
  }

  static class AllTypesRecord {
    static class Struct {
      int i;
      String s;

      Struct(int i, String s) {
        this.i = i;
        this.s = s;
      }
    }
    boolean b;
    byte bt;
    short s;
    int i;
    long l;
    float f;
    double d;
    HiveDecimal de;
    Timestamp t;
    Date dt;
    String str;
    HiveChar c;
    HiveVarchar vc;
    Map<String, String> m;
    List<Integer> a;
    Struct st;

    AllTypesRecord(boolean b, byte bt, short s, int i, long l, float f, double d, HiveDecimal de,
                   Timestamp t, Date dt, String str, HiveChar c, HiveVarchar vc, Map<String,
                   String> m, List<Integer> a, Struct st) {
      this.b = b;
      this.bt = bt;
      this.s = s;
      this.i = i;
      this.l = l;
      this.f = f;
      this.d = d;
      this.de = de;
      this.t = t;
      this.dt = dt;
      this.str = str;
      this.c = c;
      this.vc = vc;
      this.m = m;
      this.a = a;
      this.st = st;
    }
  }

  static void checkOutput(String expected,
                                  String actual) throws Exception {
    BufferedReader eStream =
        new BufferedReader(new FileReader(HiveTestUtils.getFileFromClasspath(expected)));
    BufferedReader aStream =
        new BufferedReader(new FileReader(actual));
    String expectedLine = eStream.readLine().trim();
    while (expectedLine != null) {
      String actualLine = aStream.readLine().trim();
      System.out.println("actual:   " + actualLine);
      System.out.println("expected: " + expectedLine);
      assertEquals(expectedLine, actualLine);
      expectedLine = eStream.readLine();
      expectedLine = expectedLine == null ? null : expectedLine.trim();
    }
    assertNull(eStream.readLine());
    assertNull(aStream.readLine());
    eStream.close();
    aStream.close();
  }

  @Test
  public void testDump() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.ZLIB, 10000, 1000);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]));
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=1,2,3"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testDataDump() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (AllTypesRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.NONE, 10000, 1000);
    Map<String, String> m = new HashMap<String, String>(2);
    m.put("k1", "v1");
    writer.addRow(new AllTypesRecord(
        true,
        (byte) 10,
        (short) 100,
        1000,
        10000L,
        4.0f,
        20.0,
        HiveDecimal.create("4.2222"),
        new Timestamp(1416967764000L),
        new Date(1416967764000L),
        "string",
        new HiveChar("hello", 5),
        new HiveVarchar("hello", 10),
        m,
        Arrays.asList(100, 200),
        new AllTypesRecord.Struct(10, "foo")));
    m.clear();
    m.put("k3", "v3");
    writer.addRow(new AllTypesRecord(
        false,
        (byte)20,
        (short)200,
        2000,
        20000L,
        8.0f,
        40.0,
        HiveDecimal.create("2.2222"),
        new Timestamp(1416967364000L),
        new Date(1411967764000L),
        "abcd",
        new HiveChar("world", 5),
        new HiveVarchar("world", 10),
        m,
        Arrays.asList(200, 300),
        new AllTypesRecord.Struct(20, "bar")));

    writer.close();
    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "-d"});
    System.out.flush();
    System.setOut(origOut);

    String[] lines = myOut.toString().split("\n");
    // Don't be fooled by the big space in the middle, this line is quite long
    assertEquals("{\"b\":true,\"bt\":10,\"s\":100,\"i\":1000,\"l\":10000,\"f\":4,\"d\":20,\"de\":\"4.2222\",\"t\":\"2014-11-25 18:09:24\",\"dt\":\"2014-11-25\",\"str\":\"string\",\"c\":\"hello                                                                                                                                                                                                                                                          \",\"vc\":\"hello\",\"m\":[{\"_key\":\"k1\",\"_value\":\"v1\"}],\"a\":[100,200],\"st\":{\"i\":10,\"s\":\"foo\"}}", lines[0]);
    assertEquals("{\"b\":false,\"bt\":20,\"s\":200,\"i\":2000,\"l\":20000,\"f\":8,\"d\":40,\"de\":\"2.2222\",\"t\":\"2014-11-25 18:02:44\",\"dt\":\"2014-09-28\",\"str\":\"abcd\",\"c\":\"world                                                                                                                                                                                                                                                          \",\"vc\":\"world\",\"m\":[{\"_key\":\"k3\",\"_value\":\"v3\"}],\"a\":[200,300],\"st\":{\"i\":20,\"s\":\"bar\"}}", lines[1]);
  }
  
  @Test(expected = IOException.class)
  public void testDataDumpThrowsIOException() throws Exception {
    PrintStream origOut = System.out;
    try {
      ObjectInspector inspector;
      synchronized (TestOrcFile.class) {
        inspector = ObjectInspectorFactory.getReflectionObjectInspector
            (AllTypesRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      }
      Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
          100000, CompressionKind.NONE, 10000, 1000);
      Map<String, String> m = new HashMap<String, String>(2);
      m.put("k1", "v1");
      writer.addRow(new AllTypesRecord(
          true,
          (byte) 10,
          (short) 100,
          1000,
          10000L,
          4.0f,
          20.0,
          HiveDecimal.create("4.2222"),
          new Timestamp(1416967764000L),
          new Date(1416967764000L),
          "string",
          new HiveChar("hello", 5),
          new HiveVarchar("hello", 10),
          m,
          Arrays.asList(100, 200),
          new AllTypesRecord.Struct(10, "foo")));
      
      writer.close();
      
      OutputStream myOut = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          throw new IOException();
        }
      };
      
      // replace stdout and run command
      System.setOut(new PrintStream(myOut));
      FileDump.main(new String[]{testFilePath.toString(), "-d"});
    } finally {
      System.setOut(origOut);
    }
  }

  // Test that if the fraction of rows that have distinct strings is greater than the configured
  // threshold dictionary encoding is turned off.  If dictionary encoding is turned off the length
  // of the dictionary stream for the column will be 0 in the ORC file dump.
  @Test
  public void testDictionaryThreshold() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Configuration conf = new Configuration();
    conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD.varname, 0.49f);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.ZLIB, 10000, 1000);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    int nextInt = 0;
    for(int i=0; i < 21000; ++i) {
      // Write out the same string twice, this guarantees the fraction of rows with
      // distinct strings is 0.5
      if (i % 2 == 0) {
        nextInt = r1.nextInt(words.length);
        // Append the value of i to the word, this guarantees when an index or word is repeated
        // the actual string is unique.
        words[nextInt] += "-" + i;
      }
      writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
          words[nextInt]));
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-dictionary-threshold.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=1,2,3"});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testBloomFilter() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .inspector(inspector)
        .stripeSize(100000)
        .compress(CompressionKind.ZLIB)
        .bufferSize(10000)
        .rowIndexStride(1000)
        .bloomFilterColumns("S");
    Writer writer = OrcFile.createWriter(testFilePath, options);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]));
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-bloomfilter.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=3"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testBloomFilter2() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .inspector(inspector)
        .stripeSize(100000)
        .compress(CompressionKind.ZLIB)
        .bufferSize(10000)
        .rowIndexStride(1000)
        .bloomFilterColumns("l")
        .bloomFilterFpp(0.01);
    Writer writer = OrcFile.createWriter(testFilePath, options);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]));
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-bloomfilter2.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=2"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  public static class SimpleStruct {
    BytesWritable bytes1;
    Text string1;

    SimpleStruct(BytesWritable b1, String s1) {
      this.bytes1 = b1;
      if(s1 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s1);
      }
    }
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  @Test
  public void testHasNull() throws Exception {

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .rowIndexStride(1000)
            .stripeSize(10000)
            .bufferSize(10000));
    // STRIPE 1
    // RG1
    for(int i=0; i<1000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), "RG1"));
    }
    // RG2
    for(int i=0; i<1000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), null));
    }
    // RG3
    for(int i=0; i<1000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), "RG3"));
    }
    // RG4
    for(int i=0; i<1000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), null));
    }
    // RG5
    for(int i=0; i<1000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), null));
    }
    // STRIPE 2
    for(int i=0; i<5000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), null));
    }
    // STRIPE 3
    for(int i=0; i<5000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), "STRIPE-3"));
    }
    // STRIPE 4
    for(int i=0; i<5000; i++) {
      writer.addRow(new SimpleStruct(bytes(1,2,3), null));
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check the file level stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.assertEquals(20000, stats[0].getNumberOfValues());
    Assert.assertEquals(20000, stats[1].getNumberOfValues());
    Assert.assertEquals(7000, stats[2].getNumberOfValues());
    Assert.assertEquals(false, stats[0].hasNull());
    Assert.assertEquals(false, stats[1].hasNull());
    Assert.assertEquals(true, stats[2].hasNull());

    // check the stripe level stats
    List<StripeStatistics> stripeStats = reader.getMetadata().getStripeStatistics();
    // stripe 1 stats
    StripeStatistics ss1 = stripeStats.get(0);
    ColumnStatistics ss1_cs1 = ss1.getColumnStatistics()[0];
    ColumnStatistics ss1_cs2 = ss1.getColumnStatistics()[1];
    ColumnStatistics ss1_cs3 = ss1.getColumnStatistics()[2];
    Assert.assertEquals(false, ss1_cs1.hasNull());
    Assert.assertEquals(false, ss1_cs2.hasNull());
    Assert.assertEquals(true, ss1_cs3.hasNull());

    // stripe 2 stats
    StripeStatistics ss2 = stripeStats.get(1);
    ColumnStatistics ss2_cs1 = ss2.getColumnStatistics()[0];
    ColumnStatistics ss2_cs2 = ss2.getColumnStatistics()[1];
    ColumnStatistics ss2_cs3 = ss2.getColumnStatistics()[2];
    Assert.assertEquals(false, ss2_cs1.hasNull());
    Assert.assertEquals(false, ss2_cs2.hasNull());
    Assert.assertEquals(true, ss2_cs3.hasNull());

    // stripe 3 stats
    StripeStatistics ss3 = stripeStats.get(2);
    ColumnStatistics ss3_cs1 = ss3.getColumnStatistics()[0];
    ColumnStatistics ss3_cs2 = ss3.getColumnStatistics()[1];
    ColumnStatistics ss3_cs3 = ss3.getColumnStatistics()[2];
    Assert.assertEquals(false, ss3_cs1.hasNull());
    Assert.assertEquals(false, ss3_cs2.hasNull());
    Assert.assertEquals(false, ss3_cs3.hasNull());

    // stripe 4 stats
    StripeStatistics ss4 = stripeStats.get(3);
    ColumnStatistics ss4_cs1 = ss4.getColumnStatistics()[0];
    ColumnStatistics ss4_cs2 = ss4.getColumnStatistics()[1];
    ColumnStatistics ss4_cs3 = ss4.getColumnStatistics()[2];
    Assert.assertEquals(false, ss4_cs1.hasNull());
    Assert.assertEquals(false, ss4_cs2.hasNull());
    Assert.assertEquals(true, ss4_cs3.hasNull());

    // Test file dump
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-has-null.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=2"});
    System.out.flush();
    System.setOut(origOut);

    TestFileDump.checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }
}
