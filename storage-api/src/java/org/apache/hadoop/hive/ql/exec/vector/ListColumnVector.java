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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

/**
 * The representation of a vectorized column of list objects.
 *
 * Each list is composed of a range of elements in the underlying child
 * ColumnVector. The range for list i is offsets[i]..offsets[i+1]-1 inclusive.
 */
public class ListColumnVector extends ColumnVector {

  public long[] offsets;
  public ColumnVector child;

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param len Vector length
   */
  public ListColumnVector(int len) {
    super(len);
    offsets = new long[len+1];
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      long repeatVal = offsets[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          vector[i] = repeatVal;
        }
      } else {
        Arrays.fill(vector, 0, size, repeatVal);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum,
                         ColumnVector inputVector) {
    ColumnVector[] inputFields = ((StructColumnVector) inputVector).fields;
    for(int i=0; i < inputFields.length; ++i) {
      fields[i].setElement(outElementNum, inputElementNum, inputFields[i]);
    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append('{');
      for(int i=0; i < fields.length; ++i) {
        if (i != 0) {
          buffer.append(", ");
        }
        fields[i].stringifyValue(buffer, row);
      }
      buffer.append('}');
    } else {
      buffer.append("null");
    }
  }
}
