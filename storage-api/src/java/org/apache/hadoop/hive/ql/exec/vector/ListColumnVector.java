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
 * ColumnVector. The range for list i is
 * offsets[i]..offsets[i]+lengths[i]-1 inclusive.
 */
public class ListColumnVector extends ColumnVector {

  public long[] offsets;
  public long[] lengths;
  // the number of children slots used
  public int childCount;
  public ColumnVector child;

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param len Vector length
   */
  public ListColumnVector(int len) {
    super(len);
    childCount = 0;
    offsets = new long[len];
    lengths = new long[len];
  }

  /**
   * Translate the selected list into the child ColumnVector.
   * @param offsets the list of offsets for the elements of each list
   * @param lengths the list of lengths for the elements of each list
   * @param selected the list of selected rows
   * @param size the number of rows selected
   * @return the list of the selected children
   */
  static int[] projectSelectedList(long[] offsets,
                                   long[] lengths,
                                   int[] selected, int size) {
    int numChildren = 0;
    for(int i=0; i < size; ++i) {
      numChildren += lengths[selected[i]];
    }
    int[] result = new int[numChildren];
    int next = 0;
    for(int i=0; i < size; ++i) {
      int row = selected[i];
      for(long child = offsets[row]; child < offsets[row] + lengths[row];
          ++child) {
        result[next++] = (int) child;
      }
    }
    return result;
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();

    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        if (selectedInUse) {
          for (int i = 0; i < size; ++i) {
            int row = sel[i];
            offsets[row] = offsets[0];
            lengths[row] = lengths[0];
            isNull[row] = false;
          }
        } else {
          Arrays.fill(offsets, 0, size, offsets[0]);
          Arrays.fill(lengths, 0, size, lengths[0]);
          Arrays.fill(isNull, 0, size, false);
        }
        int[] childSelection = new int[(int) lengths[0]];
        for(int i=0; i < lengths[0]; ++i) {
          childSelection[i] = (int) offsets[0] + i;
        }
        child.flatten(true, childSelection, childSelection.length);
      } else {
        if (selectedInUse) {
          for(int i=0; i < size; ++i) {
            isNull[sel[i]] = true;
          }
        } else {
          Arrays.fill(isNull, 0, size, true);
        }
      }
      isRepeating = false;
      noNulls = false;
    } else {
      if (selectedInUse) {
        int childSize = 0;
        for(int i=0; i < size; ++i) {
          childSize += lengths[sel[i]];
        }
        int[] childSelection = new int[childSize];
        int idx = 0;
        for(int i=0; i < size; ++i) {
          int row = sel[i];
          for(int elem=0; elem < lengths[row]; ++elem) {
            childSelection[idx++] = (int) (offsets[row] + elem);
          }
        }
        child.flatten(true, childSelection, childSize);
      } else {
        child.flatten(false, null, childCount);
      }
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum,
                         ColumnVector inputVector) {
    ListColumnVector input = (ListColumnVector) inputVector;
    if (lengths[outElementNum] <= input.lengths[inputElementNum]) {
      lengths[outElementNum] = input.lengths[inputElementNum];
      for(int i=0; i < lengths[outElementNum]; ++i) {
        child.setElement((int) offsets[outElementNum] + i,
            (int) input.offsets[inputElementNum] + i, input.child);
      }
    } else {
      int length = (int) input.lengths[inputElementNum];
      offsets[outElementNum] = childCount;
      childCount += length;
      lengths[outElementNum] = length;
      child.ensureSize(childCount, true);

    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append('[');
      boolean isFirst = true;
      for(long i=offsets[row]; i < offsets[row] + lengths[row]; ++i) {
        if (isFirst) {
          isFirst = false;
        } else {
          buffer.append(", ");
        }
        child.stringifyValue(buffer, (int) i);
      }
      buffer.append(']');
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    if (size > offsets.length) {
      super.ensureSize(size, preserveData);
      long[] oldOffsets = offsets;
      offsets = new long[size];
      long oldLengths[] = lengths;
      lengths = new long[size];
      if (preserveData) {
        if (isRepeating) {
          offsets[0] = oldOffsets[0];
          lengths[0] = oldLengths[0];
        } else {
          System.arraycopy(oldOffsets, 0, offsets, 0 , oldOffsets.length);
          System.arraycopy(oldLengths, 0, lengths, 0, oldLengths.length);
        }
      }
    }
  }

  @Override
  public void init() {
    super.init();
    child.init();
  }

  @Override
  public void reset() {
    super.reset();
    child.reset();
  }
}
