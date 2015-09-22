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

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testLongMerge() throws Exception {
    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.IntegerStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.IntegerStatisticsImpl();
    stats1.updateInteger(10);
    stats1.updateInteger(10);
    stats2.updateInteger(1);
    stats2.updateInteger(1000);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
    stats1.reset();
    stats1.updateInteger(-10);
    stats1.updateInteger(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum());
    assertEquals(10000, typed.getMaximum());
  }

  @Test
  public void testDoubleMerge() throws Exception {
    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.DoubleStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.DoubleStatisticsImpl();
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
    stats1.reset();
    stats1.updateDouble(-10);
    stats1.updateDouble(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum(), 0.001);
    assertEquals(10000, typed.getMaximum(), 0.001);
  }


  @Test
  public void testStringMerge() throws Exception {
    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.StringStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.StringStatisticsImpl();
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.updateString(new Text("anne"));
    stats2.updateString(new Text("erin"));
    stats1.merge(stats2);
    StringColumnStatistics typed = (StringColumnStatistics) stats1;
    assertEquals("anne", typed.getMinimum());
    assertEquals("erin", typed.getMaximum());
    stats1.reset();
    stats1.updateString(new Text("aaa"));
    stats1.updateString(new Text("zzz"));
    stats1.merge(stats2);
    assertEquals("aaa", typed.getMinimum());
    assertEquals("zzz", typed.getMaximum());
  }

  @Test
  public void testDateMerge() throws Exception {
    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.DateStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.DateStatisticsImpl();
    stats1.updateDate(1000);
    stats1.updateDate(100);
    stats2.updateDate(10);
    stats2.updateDate(2000);
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(new Date(70, 0, 11), typed.getMinimum());
    assertEquals(new Date(75, 5, 24), typed.getMaximum());
    stats1.reset();
    stats1.updateDate(-10);
    stats1.updateDate(10000);
    stats1.merge(stats2);
    assertEquals(new Date(69, 11, 22), typed.getMinimum());
    assertEquals(new Date(97, 4, 19), typed.getMaximum());
  }

  @Test
  public void testTimestampMerge() throws Exception {

    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.TimestampStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.TimestampStatisticsImpl();
    stats1.updateTimestamp(new Timestamp(10));
    stats1.updateTimestamp(new Timestamp(100));
    stats2.updateTimestamp(new Timestamp(1));
    stats2.updateTimestamp(new Timestamp(1000));
    stats1.merge(stats2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().getTime());
    assertEquals(1000, typed.getMaximum().getTime());
    stats1.reset();
    stats1.updateTimestamp(new Timestamp(-10));
    stats1.updateTimestamp(new Timestamp(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getTime());
    assertEquals(10000, typed.getMaximum().getTime());
  }

  @Test
  public void testDecimalMerge() throws Exception {
    ColumnStatisticsImpl stats1 = new ColumnStatisticsImpl.DecimalStatisticsImpl();
    ColumnStatisticsImpl stats2 = new ColumnStatisticsImpl.DecimalStatisticsImpl();
    stats1.updateDecimal(HiveDecimal.create(10));
    stats1.updateDecimal(HiveDecimal.create(100));
    stats2.updateDecimal(HiveDecimal.create(1));
    stats2.updateDecimal(HiveDecimal.create(1000));
    stats1.merge(stats2);
    DecimalColumnStatistics typed = (DecimalColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().longValue());
    assertEquals(1000, typed.getMaximum().longValue());
    stats1.reset();
    stats1.updateDecimal(HiveDecimal.create(-10));
    stats1.updateDecimal(HiveDecimal.create(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().longValue());
    assertEquals(10000, typed.getMaximum().longValue());
  }


  public static class SimpleStruct {
    BytesWritable bytes1;
    Text string1;

    SimpleStruct(BytesWritable b1, String s1) {
      this.bytes1 = b1;
      if (s1 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s1);
      }
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for (int i = 0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

}
