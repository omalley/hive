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
   * For the simple cases, find the number of children elements. The simple case
   * is where each of the lists is laid out end to end in order in the children
   * array.
   * @param offsets the start of each list
   * @param lengths the length of each list
   * @param size the number of lists
   * @return If the list is simple, it will be the number of children, otherwise
   *   a -1 is returned.
   */
  static int findChildLength(long[] offsets, long[] lengths, int size) {
    int result = 0;
    for(int i=0; i < size; ++i) {
      if (offsets[i] != result) {
        return -1;
      }
      result += lengths[i];
    }
    return result;
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

    // figure out if the layout is the simple one and make it simple if not
    int numChildren = -1;
    int[] childSelection = null;
    if (!selectedInUse) {
      numChildren = findChildLength(offsets, lengths, size);
      if (numChildren == -1) {
        selectedInUse = true;
        sel = new int[size];
        for (int i = 0; i < size; ++i) {
          sel[i] = i;
        }
      }
    }

    // flatten the children
    if (selectedInUse) {
      childSelection = projectSelectedList(offsets, lengths, sel, size);
      numChildren = childSelection.length;
      child.flatten(true, childSelection, numChildren);
    } else {
      child.flatten(false, null, numChildren);
    }

    if (isRepeating) {
      isRepeating = false;
      if (selectedInUse) {
        int cur = 0;
        for(int i=0; i < size; ++i) {
          int row = sel[i];
          offsets[row] = childSelection[cur];
          lengths[row] = lengths[0];
          cur += lengths[0];
        }
      } else {
        int cur = 0;
        for(int i=0; i < size; ++i) {
          offsets[i] = cur;
          lengths[i] = lengths[0];
          cur += lengths[0];
        }
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
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
}
