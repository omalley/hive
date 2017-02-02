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

package org.apache.hive.beeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class TestIncrementalRowsWithNormalization {

  @Test
  public void testIncrementalRows() throws SQLException {
    Integer incrementalBufferRows = 5;

    // Mock BeeLineOpts
    BeeLineOpts mockBeeLineOpts = mock(BeeLineOpts.class);
    when(mockBeeLineOpts.getIncrementalBufferRows()).thenReturn(incrementalBufferRows);
    when(mockBeeLineOpts.getNumberFormat()).thenReturn("default");
    when(mockBeeLineOpts.getNullString()).thenReturn("NULL");

    // Mock BeeLine
    BeeLine mockBeeline = mock(BeeLine.class);
    when(mockBeeline.getOpts()).thenReturn(mockBeeLineOpts);

    // MockResultSet
    ResultSet mockResultSet = mock(ResultSet.class);

    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("Mock Table");
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    // First 10 calls to resultSet.next() should return true
    when(mockResultSet.next()).thenAnswer(new Answer<Boolean>() {
      private int iterations = 10;

      @Override
      public Boolean answer(InvocationOnMock invocation) {
        return this.iterations-- > 0;
      }
    });

    when(mockResultSet.getString(1)).thenReturn("Hello World");

    // IncrementalRows constructor should buffer the first "incrementalBufferRows" rows
    IncrementalRowsWithNormalization incrementalRowsWithNormalization = new IncrementalRowsWithNormalization(
            mockBeeline, mockResultSet);

    // When the first buffer is loaded ResultSet.next() should be called "incrementalBufferRows" times
    verify(mockResultSet, times(5)).next();

    // Iterating through the buffer should not cause the next buffer to be fetched
    for (int i = 0; i < incrementalBufferRows + 1; i++) {
      incrementalRowsWithNormalization.next();
    }
    verify(mockResultSet, times(5)).next();

    // When a new buffer is fetched ResultSet.next() should be called "incrementalBufferRows" more times
    incrementalRowsWithNormalization.next();
    verify(mockResultSet, times(10)).next();
  }
}