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

package org.apache.hadoop.hive.llap.io.metadata;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.ReaderImpl;

/** ORC file metadata. Currently contains some duplicate info due to how different parts
 * of ORC use different info. Ideally we would get rid of protobuf structs in code beyond reading,
 * or instead use protobuf structs everywhere instead of the mix of things like now.
 */
public final class OrcFileMetadata extends LlapCacheableBuffer {
  private final Object fileKey;
  private final Reader reader;

  private final int estimatedMemUsage;

  private final static HashMap<Class<?>, ObjectEstimator> SIZE_ESTIMATORS;
  private final static ObjectEstimator SIZE_ESTIMATOR;
  static {
    OrcFileMetadata ofm = createDummy(new SyntheticFileId());
    SIZE_ESTIMATORS = IncrementalObjectSizeEstimator.createEstimators(ofm);
    IncrementalObjectSizeEstimator.addEstimator(
        "com.google.protobuf.LiteralByteString", SIZE_ESTIMATORS);
    // Add long for the regular file ID estimation.
    IncrementalObjectSizeEstimator.createEstimators(Long.class, SIZE_ESTIMATORS);
    SIZE_ESTIMATOR = SIZE_ESTIMATORS.get(OrcFileMetadata.class);
  }

  @VisibleForTesting
  public static OrcFileMetadata createDummy(Object fileKey) {
    OrcFileMetadata ofm = new OrcFileMetadata(fileKey);
    return ofm;
  }

  static OrcProto.ColumnStatistics.Builder createStatsDummy() {
    return OrcProto.ColumnStatistics.newBuilder().setBucketStatistics(
            OrcProto.BucketStatistics.newBuilder().addCount(0)).setStringStatistics(
            OrcProto.StringStatistics.newBuilder().setMaximum("zzz"));
  }

  // Ctor for memory estimation and tests
  private OrcFileMetadata(Object fileKey) {
    this.fileKey = fileKey;
    reader = null;
    estimatedMemUsage = 0;
  }

  public OrcFileMetadata(Object fileKey, Reader reader) {
    this.fileKey = fileKey;
    this.reader = reader;

    this.estimatedMemUsage = SIZE_ESTIMATOR.estimate(this, SIZE_ESTIMATORS);
  }

  // LlapCacheableBuffer
  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }

  @Override
  protected boolean invalidate() {
    return true; // relies on GC, so it can always be evicted now.
  }

  @Override
  public long getMemoryUsage() {
    return estimatedMemUsage;
  }

  @Override
  protected boolean isLocked() {
    return false;
  }
}
