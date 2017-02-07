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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.SelfDescribingInputFormatInterface;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A MapReduce/Hive input format for ORC files.
 * <p>
 * This class implements both the classic InputFormat, which stores the rows
 * directly, and AcidInputFormat, which stores a series of events with the
 * following schema:
 * <pre>
 *   class AcidEvent&lt;ROW&gt; {
 *     enum ACTION {INSERT, UPDATE, DELETE}
 *     ACTION operation;
 *     long originalTransaction;
 *     int bucket;
 *     long rowId;
 *     long currentTransaction;
 *     ROW row;
 *   }
 * </pre>
 * Each AcidEvent object corresponds to an update event. The
 * originalTransaction, bucket, and rowId are the unique identifier for the row.
 * The operation and currentTransaction are the operation and the transaction
 * that added this event. Insert and update events include the entire row, while
 * delete events have null for row.
 */
public class OrcInputFormat  implements InputFormat<NullWritable, OrcStruct>,
  InputFormatChecker, VectorizedInputFormatInterface,
  SelfDescribingInputFormatInterface, AcidInputFormat<NullWritable, OrcStruct>,
  CombineHiveInputFormat.AvoidSplitCombination {

  static enum SplitStrategyKind{
    HYBRID,
    BI,
    ETL
  }

  private static final Log LOG = LogFactory.getLog(OrcInputFormat.class);
  private static boolean isDebugEnabled = LOG.isDebugEnabled();
  static final HadoopShims SHIMS = ShimLoader.getHadoopShims();
  static final String MIN_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMINSPLITSIZE");
  static final String MAX_SPLIT_SIZE =
      SHIMS.getHadoopConfNames().get("MAPREDMAXSPLITSIZE");

  private static final long DEFAULT_MIN_SPLIT_SIZE = 16 * 1024 * 1024;
  private static final long DEFAULT_MAX_SPLIT_SIZE = 256 * 1024 * 1024;
  private static final int DEFAULT_ETL_FILE_THRESHOLD = 100;
  private static final int DEFAULT_CACHE_INITIAL_CAPACITY = 1024;

  private static final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final String CLASS_NAME = ReaderImpl.class.getName();

  /**
   * When picking the hosts for a split that crosses block boundaries,
   * drop any host that has fewer than MIN_INCLUDED_LOCATION of the
   * number of bytes available on the host with the most.
   * If host1 has 10MB of the split, host2 has 20MB, and host3 has 18MB the
   * split will contain host2 (100% of host2) and host3 (90% of host2). Host1
   * with 50% will be dropped.
   */
  private static final double MIN_INCLUDED_LOCATION = 0.80;

  @Override
  public boolean shouldSkipCombine(Path path,
                                   Configuration conf) throws IOException {
    return (conf.get(AcidUtils.CONF_ACID_KEY) != null) || AcidUtils.isAcid(path, conf);
  }


  /**
   * We can derive if a split is ACID or not from the flags encoded in OrcSplit.
   * If the file split is not instance of OrcSplit then its definitely not ACID.
   * If file split is instance of OrcSplit and the flags contain hasBase or deltas then it's
   * definitely ACID.
   * Else fallback to configuration object/table property.
   * @param conf
   * @param inputSplit
   * @return
   */
  public boolean isAcidRead(Configuration conf, InputSplit inputSplit) {
    if (!(inputSplit instanceof OrcSplit)) {
      return false;
    }

    /*
     * If OrcSplit.isAcid returns true, we know for sure it is ACID.
     */
    // if (((OrcSplit) inputSplit).isAcid()) {
    //   return true;
    // }

    /*
     * Fallback for the case when OrcSplit flags do not contain hasBase and deltas
     */
    return HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
  }

  private static class OrcRecordReader
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>,
      StatsProvidingRecordReader {
    private final RecordReader reader;
    private final long offset;
    private final long length;
    private final int numColumns;
    private float progress = 0.0f;
    private final Reader file;
    private final SerDeStats stats;


    OrcRecordReader(Reader file, Configuration conf,
                    FileSplit split) throws IOException {
      List<OrcProto.Type> types = file.getTypes();
      this.file = file;
      numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
      this.offset = split.getStart();
      this.length = split.getLength();
      this.reader = createReaderFromFile(file, conf, offset, length);
      this.stats = new SerDeStats();
    }

    @Override
    public boolean next(NullWritable key, OrcStruct value) throws IOException {
      if (reader.hasNext()) {
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return new OrcStruct(numColumns);
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }

    @Override
    public SerDeStats getStats() {
      stats.setRawDataSize(file.getRawDataSize());
      stats.setRowCount(file.getNumberOfRows());
      return stats;
    }
  }

  /**
   * Get the root column for the row. In ACID format files, it is offset by
   * the extra metadata columns.
   * @param isOriginal is the file in the original format?
   * @return the column number for the root of row.
   */
  private static int getRootColumn(boolean isOriginal) {
    return isOriginal ? 0 : (OrcRecordUpdater.ROW + 1);
  }

  public static void raiseAcidTablesMustBeReadWithAcidReaderException(Configuration conf)
      throws IOException {
    String hiveInputFormat = HiveConf.getVar(conf, ConfVars.HIVEINPUTFORMAT);
    if (hiveInputFormat.equals(HiveInputFormat.class.getName())) {
      throw new IOException(ErrorMsg.ACID_TABLES_MUST_BE_READ_WITH_ACID_READER.getErrorCodedMsg());
    } else {
      throw new IOException(ErrorMsg.ACID_TABLES_MUST_BE_READ_WITH_HIVEINPUTFORMAT.getErrorCodedMsg());
    }
  }

  public static RecordReader createReaderFromFile(Reader file,
                                                  Configuration conf,
                                                  long offset, long length
                                                  ) throws IOException {

    boolean isTransactionalTableScan = HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
    if (isTransactionalTableScan) {
      raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }

    /**
     * Do we have schema on read in the configuration variables?
     */
    TypeDescription schema = OrcUtils.getDesiredRowTypeDescr(conf, /* isAcidRead */ false);

    Reader.Options options = new Reader.Options().range(offset, length);
    options.schema(schema);
    boolean isOriginal = isOriginal(file);
    if (schema == null) {
      schema = file.getSchema();
    }
    List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
    options.include(genIncludedColumns(schema, conf));
    setSearchArgument(options, types, conf, isOriginal);
    return file.rowsOptions(options);
  }

  public static boolean isOriginal(Reader file) {
    return !file.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
  }

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   * @param types the types of the file
   * @param result the global view of columns that should be included
   * @param typeId the root of tree to enable
   * @param rootColumn the top column
   */
  private static void includeColumnRecursive(List<OrcProto.Type> types,
                                             boolean[] result,
                                             int typeId,
                                             int rootColumn) {
    result[typeId - rootColumn] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i), rootColumn);
    }
  }

  public static boolean[] genIncludedColumns(TypeDescription readerSchema,
                                             List<Integer> included) {

    boolean[] result = new boolean[readerSchema.getMaximumId() + 1];
    result[0] = true;
    List<TypeDescription> children = readerSchema.getChildren();
    for (int columnNumber = 0; columnNumber < children.size(); ++columnNumber) {
      if (included.contains(columnNumber)) {
        TypeDescription child = children.get(columnNumber);
        for(int col = child.getId(); col <= child.getMaximumId(); ++col) {
          result[col] = true;
        }
      }
    }
    return result;
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param readerSchema the types for the reader
   * @param conf the configuration
   */
  public static boolean[] genIncludedColumns(TypeDescription readerSchema,
                                             Configuration conf) {
    if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      return genIncludedColumns(readerSchema, included);
    } else {
      return null;
    }
  }

  public static String[] getSargColumnNames(String[] originalColumnNames,
      List<OrcProto.Type> types, boolean[] includedColumns, boolean isOriginal) {
    int rootColumn = getRootColumn(isOriginal);
    String[] columnNames = new String[types.size() - rootColumn];
    int i = 0;
    for(int columnId: types.get(rootColumn).getSubtypesList()) {
      if (includedColumns == null || includedColumns[columnId - rootColumn]) {
        // this is guaranteed to be positive because types only have children
        // ids greater than their own id.
        columnNames[columnId - rootColumn] = originalColumnNames[i++];
      }
    }
    return columnNames;
  }

  static void setSearchArgument(Reader.Options options,
                                  List<OrcProto.Type> types,
                                  Configuration conf,
                                  boolean isOriginal) {
    setSearchArgument(options, types, conf, isOriginal, true);
  }

  static void setSearchArgument(Reader.Options options,
                                List<OrcProto.Type> types,
                                Configuration conf,
                                boolean isOriginal,
                                boolean doLogSarg) {
    String columnNamesString = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    if (columnNamesString == null) {
      LOG.debug("No ORC pushdown predicate - no column names");
      options.searchArgument(null, null);
      return;
    }
    SearchArgument sarg = SearchArgumentFactory.createFromConf(conf);
    if (sarg == null) {
      LOG.debug("No ORC pushdown predicate");
      options.searchArgument(null, null);
      return;
    }

    if (doLogSarg && LOG.isInfoEnabled()) {
      LOG.info("ORC pushdown predicate: " + sarg);
    }
    options.searchArgument(sarg, getSargColumnNames(
        columnNamesString.split(","), types, options.getInclude(), isOriginal));
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
                               List<FileStatus> files
                              ) throws IOException {

    if (Utilities.isVectorMode(conf)) {
      return new VectorizedOrcInputFormat().validateInput(fs, conf, files);
    }

    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      // 0 length files cannot be ORC files
      if (file.getLen() == 0) {
        return false;
      }
      try {
        OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(conf).filesystem(fs).maxLength(file.getLen()));
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   *
   * @param conf The configuration of the job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  static Path[] getInputPaths(Configuration conf) throws IOException {
    String dirs = conf.get("mapred.input.dir");
    if (dirs == null) {
      throw new IOException("Configuration mapred.input.dir is not defined.");
    }
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * The global information about the split generation that we pass around to
   * the different worker threads.
   */
  static class Context {
    private final Configuration conf;
    private static Cache<Path, OrcTail> footerCache;
    private static ExecutorService threadPool = null;
    private final int numBuckets;
    private final long maxSize;
    private final long minSize;
    private final boolean footerInSplits;
    private final boolean cacheStripeDetails;
    private final boolean forceThreadpool;
    private final int etlFileThreshold;
    private final AtomicInteger cacheHitCounter = new AtomicInteger(0);
    private final AtomicInteger numFilesCounter = new AtomicInteger(0);
    private ValidTxnList transactionList;
    private SplitStrategyKind splitStrategyKind;

    Context(Configuration conf) {
      this(conf, 1);
    }

    Context(Configuration conf, final int minSplits) {
      this.conf = conf;
      minSize = conf.getLong(MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
      maxSize = conf.getLong(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);
      this.forceThreadpool = HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST);
      String ss = conf.get(ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname);
      if (ss == null || ss.equals(SplitStrategyKind.HYBRID.name())) {
        splitStrategyKind = SplitStrategyKind.HYBRID;
      } else {
        LOG.info("Enforcing " + ss + " ORC split strategy");
        splitStrategyKind = SplitStrategyKind.valueOf(ss);
      }
      footerInSplits = HiveConf.getBoolVar(conf,
          ConfVars.HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS);
      numBuckets =
          Math.max(conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0), 0);
      LOG.debug("Number of buckets specified by conf file is " + numBuckets);
      int cacheStripeDetailsSize = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE);
      int numThreads = HiveConf.getIntVar(conf,
          ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS);
      boolean useSoftReference = HiveConf.getBoolVar(conf,
              ConfVars.HIVE_ORC_CACHE_USE_SOFT_REFERENCES);

      cacheStripeDetails = (cacheStripeDetailsSize > 0);

      this.etlFileThreshold = minSplits <= 0 ? DEFAULT_ETL_FILE_THRESHOLD : minSplits;

      synchronized (Context.class) {
        if (threadPool == null) {
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true)
                  .setNameFormat("ORC_GET_SPLITS #%d").build());
        }

        if (footerCache == null && cacheStripeDetails) {
          CacheBuilder builder = CacheBuilder.newBuilder()
                  .initialCapacity(DEFAULT_CACHE_INITIAL_CAPACITY)
                  .concurrencyLevel(numThreads)
                  .maximumSize(cacheStripeDetailsSize);
          if (useSoftReference) {
            builder = builder.softValues();
          }
          footerCache = builder.build();
        }
      }
      String value = conf.get(ValidTxnList.VALID_TXNS_KEY);
      transactionList = value == null ? new ValidReadTxnList() : new ValidReadTxnList(value);
    }

    @VisibleForTesting
    static int getCurrentThreadPoolSize() {
      synchronized (Context.class) {
        return (threadPool instanceof ThreadPoolExecutor)
            ? ((ThreadPoolExecutor)threadPool).getPoolSize() : ((threadPool == null) ? 0 : -1);
      }
    }
    @VisibleForTesting
    static void resetThreadPool() {
      synchronized (Context.class) {
        threadPool = null;
      }
    }
  }

  interface SplitStrategy<T> {
    List<T> getSplits() throws IOException;
  }

  static final class SplitInfo extends ACIDSplitStrategy {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final OrcTail orcTail;
    private final List<OrcProto.Type> readerTypes;
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;

    SplitInfo(Context context, FileSystem fs,
        FileStatus file, OrcTail orcTail,
        List<OrcProto.Type> readerTypes,
        boolean isOriginal,
        List<DeltaMetaData> deltas,
        boolean hasBase, Path dir, boolean[] covered) throws IOException {
      super(dir, context.numBuckets, deltas, covered);
      this.context = context;
      this.fs = fs;
      this.file = file;
      this.orcTail = orcTail;
      this.readerTypes = readerTypes;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.hasBase = hasBase;
    }
  }

  /**
   * ETL strategy is used when spending little more time in split generation is acceptable
   * (split generation reads and caches file footers).
   */
  static final class ETLSplitStrategy implements SplitStrategy<SplitInfo> {
    Context context;
    FileSystem fs;
    List<FileStatus> files;
    List<OrcProto.Type> readerTypes;
    boolean isOriginal;
    List<DeltaMetaData> deltas;
    Path dir;
    boolean[] covered;

    public ETLSplitStrategy(Context context, FileSystem fs, Path dir, List<FileStatus> children,
        List<OrcProto.Type> readerTypes, boolean isOriginal, List<DeltaMetaData> deltas, boolean[] covered) {
      this.context = context;
      this.dir = dir;
      this.fs = fs;
      this.files = children;
      this.readerTypes = readerTypes;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.covered = covered;
    }

    private OrcTail verifyCachedOrcTail(FileStatus file) {
      context.numFilesCounter.incrementAndGet();
      OrcTail orcTail = Context.footerCache.getIfPresent(file.getPath());
      if (orcTail != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Info cached for path: " + file.getPath());
        }
        if (orcTail.getFileModificationTime() == file.getModificationTime() &&
            orcTail.getFileLength() == file.getLen()) {
          // Cached copy is valid
          context.cacheHitCounter.incrementAndGet();
          return orcTail;
        } else {
          // Invalidate
          Context.footerCache.invalidate(file.getPath());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Meta-Info for : " + file.getPath() +
                " changed. CachedModificationTime: "
                + orcTail.getFileModificationTime() + ", CurrentModificationTime: "
                + file.getModificationTime()
                + ", CachedLength: " + orcTail.getFileLength() + ", CurrentLength: " +
                file.getLen());
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Info not cached for path: " + file.getPath());
        }
      }
      return null;
    }

    @Override
    public List<SplitInfo> getSplits() throws IOException {
      List<SplitInfo> result = Lists.newArrayList();
      for (FileStatus file : files) {
        OrcTail orcTail = null;
        if (context.cacheStripeDetails) {
          orcTail = verifyCachedOrcTail(file);
        }
        // ignore files of 0 length
        if (file.getLen() > 0) {
          result.add(new SplitInfo(context, fs, file, orcTail, readerTypes, isOriginal, deltas,
              true, dir, covered));
        }
      }
      return result;
    }

    @Override
    public String toString() {
      return ETLSplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * BI strategy is used when the requirement is to spend less time in split generation
   * as opposed to query execution (split generation does not read or cache file footers).
   */
  static final class BISplitStrategy extends ACIDSplitStrategy {
    List<FileStatus> fileStatuses;
    boolean isOriginal;
    List<DeltaMetaData> deltas;
    FileSystem fs;
    Path dir;

    public BISplitStrategy(Context context, FileSystem fs,
        Path dir, List<FileStatus> fileStatuses, List<OrcProto.Type> readerTypes,
        boolean isOriginal, List<DeltaMetaData> deltas, boolean[] covered) {
      super(dir, context.numBuckets, deltas, covered);
      this.fileStatuses = fileStatuses;
      this.isOriginal = isOriginal;
      this.deltas = deltas;
      this.fs = fs;
      this.dir = dir;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      List<OrcSplit> splits = Lists.newArrayList();
      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.getLen() != 0) {
          TreeMap<Long, BlockLocation> blockOffsets = SHIMS.getLocationsWithOffset(fs, fileStatus);
          for (Map.Entry<Long, BlockLocation> entry : blockOffsets.entrySet()) {
            OrcSplit orcSplit = new OrcSplit(fileStatus.getPath(), entry.getKey(),
                  entry.getValue().getLength(), entry.getValue().getHosts(), null, isOriginal, true,
                  deltas, -1, fileStatus.getLen());
            splits.add(orcSplit);
          }
        }
      }

      // add uncovered ACID delta splits
      splits.addAll(super.getSplits());
      return splits;
    }

    @Override
    public String toString() {
      return BISplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * ACID split strategy is used when there is no base directory (when transactions are enabled).
   */
  static class ACIDSplitStrategy implements SplitStrategy<OrcSplit> {
    Path dir;
    List<DeltaMetaData> deltas;
    boolean[] covered;
    int numBuckets;

    public ACIDSplitStrategy(Path dir, int numBuckets, List<DeltaMetaData> deltas, boolean[] covered) {
      this.dir = dir;
      this.numBuckets = numBuckets;
      this.deltas = deltas;
      this.covered = covered;
    }

    @Override
    public List<OrcSplit> getSplits() throws IOException {
      // Generate a split for any buckets that weren't covered.
      // This happens in the case where a bucket just has deltas and no
      // base.
      List<OrcSplit> splits = Lists.newArrayList();
      if (!deltas.isEmpty()) {
        for (int b = 0; b < numBuckets; ++b) {
          if (!covered[b]) {
            splits.add(new OrcSplit(dir, b, 0, new String[0], null, false, false, deltas, -1, -1));
          }
        }
      }
      return splits;
    }

    @Override
    public String toString() {
      return ACIDSplitStrategy.class.getSimpleName() + " strategy for " + dir;
    }
  }

  /**
   * Given a directory, get the list of files and blocks in those files.
   * To parallelize file generator use "mapreduce.input.fileinputformat.list-status.num-threads"
   */
  static final class FileGenerator implements Callable<SplitStrategy> {
    private final Context context;
    private final FileSystem fs;
    private final Path dir;
    private final List<OrcProto.Type> readerTypes;
    private final UserGroupInformation ugi;

    FileGenerator(Context context, FileSystem fs, Path dir, List<OrcProto.Type> readerTypes,
        UserGroupInformation ugi) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
      this.readerTypes = readerTypes;
      this.ugi = ugi;
    }

    @Override
    public SplitStrategy call() throws IOException {
      if (ugi == null) {
        return callInternal();
      }
    try {
      return ugi.doAs(new PrivilegedExceptionAction<SplitStrategy>() {
        @Override
        public SplitStrategy run() throws Exception {
          return callInternal();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    }

    private SplitStrategy callInternal() throws IOException {
        final SplitStrategy splitStrategy;
      AcidUtils.Directory dirInfo = AcidUtils.getAcidState(dir,
          context.conf, context.transactionList, true);
      List<DeltaMetaData> deltas = AcidUtils.serializeDeltas(dirInfo.getCurrentDirectories());
      Path base = dirInfo.getBaseDirectory();
      List<FileStatus> original = dirInfo.getOriginalFiles();
      boolean[] covered = new boolean[context.numBuckets];
      boolean isOriginal = base == null;

      // if we have a base to work from
      if (base != null || !original.isEmpty()) {

        // find the base files (original or new style)
        List<FileStatus> children = original;
        if (base != null) {
          children = SHIMS.listLocatedStatus(fs, base,
              AcidUtils.hiddenFileFilter);
        }

        long totalFileSize = 0;
        for (FileStatus child : children) {
          totalFileSize += child.getLen();
          AcidOutputFormat.Options opts = AcidUtils.parseBaseBucketFilename
              (child.getPath(), context.conf);
          int b = opts.getBucket();
          // If the bucket is in the valid range, mark it as covered.
          // I wish Hive actually enforced bucketing all of the time.
          if (b >= 0 && b < covered.length) {
            covered[b] = true;
          }
        }

        int numFiles = children.size();
        long avgFileSize = totalFileSize / numFiles;
        int totalFiles = context.numFilesCounter.addAndGet(numFiles);
        switch(context.splitStrategyKind) {
          case BI:
            // BI strategy requested through config
            splitStrategy = new BISplitStrategy(context, fs, dir, children, readerTypes,
                isOriginal, deltas, covered);
            break;
          case ETL:
            // ETL strategy requested through config
            splitStrategy = new ETLSplitStrategy(context, fs, dir, children, readerTypes,
                isOriginal, deltas, covered);
            break;
          default:
            // HYBRID strategy
            if (avgFileSize > context.maxSize || totalFiles <= context.etlFileThreshold) {
              splitStrategy = new ETLSplitStrategy(context, fs, dir, children, readerTypes,
                  isOriginal, deltas, covered);
            } else {
              splitStrategy = new BISplitStrategy(context, fs, dir, children, readerTypes,
                  isOriginal, deltas, covered);
            }
            break;
        }
      } else {
        // no base, only deltas
        splitStrategy = new ACIDSplitStrategy(dir, context.numBuckets, deltas, covered);
      }

      return splitStrategy;
    }
  }

  /**
   * Split the stripes of a given file into input splits.
   * A thread is used for each file.
   */
  static final class SplitGenerator implements Callable<List<OrcSplit>> {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final long blockSize;
    private final TreeMap<Long, BlockLocation> locations;
    private OrcTail orcTail;
    private List<OrcProto.Type> readerTypes;
    private List<StripeInformation> stripes;
    private List<StripeStatistics> stripeStats;
    private List<OrcProto.Type> fileTypes;
    private boolean[] included;          // The included columns from the Hive configuration.
    private boolean[] readerIncluded;    // The included columns of the reader / file schema that
                                         // include ACID columns if present.
    private final boolean isOriginal;
    private final List<DeltaMetaData> deltas;
    private final boolean hasBase;
    private OrcFile.WriterVersion writerVersion;
    private long projColsUncompressedSize;
    private List<OrcSplit> deltaSplits;
    private final UserGroupInformation ugi;
    private SchemaEvolution evolution;

    public SplitGenerator(SplitInfo splitInfo, UserGroupInformation ugi) throws IOException {
      this.context = splitInfo.context;
      this.fs = splitInfo.fs;
      this.file = splitInfo.file;
      this.blockSize = file.getBlockSize();
      this.orcTail = splitInfo.orcTail;
      this.readerTypes = splitInfo.readerTypes;
      locations = SHIMS.getLocationsWithOffset(fs, file);
      this.isOriginal = splitInfo.isOriginal;
      this.deltas = splitInfo.deltas;
      this.hasBase = splitInfo.hasBase;
      this.projColsUncompressedSize = -1;
      this.deltaSplits = splitInfo.getSplits();
      this.ugi = ugi;
    }

    Path getPath() {
      return file.getPath();
    }

    @Override
    public String toString() {
      return "splitter(" + file.getPath() + ")";
    }

    /**
     * Compute the number of bytes that overlap between the two ranges.
     * @param offset1 start of range1
     * @param length1 length of range1
     * @param offset2 start of range2
     * @param length2 length of range2
     * @return the number of bytes in the overlap range
     */
    static long getOverlap(long offset1, long length1,
                           long offset2, long length2) {
      long end1 = offset1 + length1;
      long end2 = offset2 + length2;
      if (end2 <= offset1 || end1 <= offset2) {
        return 0;
      } else {
        return Math.min(end1, end2) - Math.max(offset1, offset2);
      }
    }

    /**
     * Create an input split over the given range of bytes. The location of the
     * split is based on where the majority of the byte are coming from. ORC
     * files are unlikely to have splits that cross between blocks because they
     * are written with large block sizes.
     * @param offset the start of the split
     * @param length the length of the split
     * @param orcTail orc file tail
     * @throws IOException
     */
    OrcSplit createSplit(long offset, long length,
                     OrcTail orcTail) throws IOException {
      String[] hosts;
      Map.Entry<Long, BlockLocation> startEntry = locations.floorEntry(offset);
      BlockLocation start = startEntry.getValue();
      if (offset + length <= start.getOffset() + start.getLength()) {
        // handle the single block case
        hosts = start.getHosts();
      } else {
        Map.Entry<Long, BlockLocation> endEntry = locations.floorEntry(offset + length);
        BlockLocation end = endEntry.getValue();
        //get the submap
        NavigableMap<Long, BlockLocation> navigableMap = locations.subMap(startEntry.getKey(),
                  true, endEntry.getKey(), true);
        // Calculate the number of bytes in the split that are local to each
        // host.
        Map<String, LongWritable> sizes = new HashMap<String, LongWritable>();
        long maxSize = 0;
        for (BlockLocation block : navigableMap.values()) {
          long overlap = getOverlap(offset, length, block.getOffset(),
              block.getLength());
          if (overlap > 0) {
            for(String host: block.getHosts()) {
              LongWritable val = sizes.get(host);
              if (val == null) {
                val = new LongWritable();
                sizes.put(host, val);
              }
              val.set(val.get() + overlap);
              maxSize = Math.max(maxSize, val.get());
            }
          } else {
            throw new IOException("File " + file.getPath().toString() +
                    " should have had overlap on block starting at " + block.getOffset());
          }
        }
        // filter the list of locations to those that have at least 80% of the
        // max
        long threshold = (long) (maxSize * MIN_INCLUDED_LOCATION);
        List<String> hostList = new ArrayList<String>();
        // build the locations in a predictable order to simplify testing
        for(BlockLocation block: navigableMap.values()) {
          for(String host: block.getHosts()) {
            if (sizes.containsKey(host)) {
              if (sizes.get(host).get() >= threshold) {
                hostList.add(host);
              }
              sizes.remove(host);
            }
          }
        }
        hosts = new String[hostList.size()];
        hostList.toArray(hosts);
      }

      // scale the raw data size to split level based on ratio of split wrt to file length
      final long fileLen = file.getLen();
      final double splitRatio = (double) length / (double) fileLen;
      final long scaledProjSize = projColsUncompressedSize > 0 ?
          (long) (splitRatio * projColsUncompressedSize) : fileLen;
      return new OrcSplit(file.getPath(), offset, length, hosts, orcTail,
          isOriginal, hasBase, deltas, scaledProjSize, fileLen);
    }

    /**
     * Divide the adjacent stripes in the file into input splits based on the
     * block size and the configured minimum and maximum sizes.
     */
    @Override
    public List<OrcSplit> call() throws IOException {
      if (ugi == null) {
        return callInternal();
      }
      try {
        return ugi.doAs(new PrivilegedExceptionAction<List<OrcSplit>>() {
          @Override
          public List<OrcSplit> run() throws Exception {
            return callInternal();
          }
        });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    private List<OrcSplit> callInternal() throws IOException {
      populateAndCacheStripeDetails();
      List<OrcSplit> splits = Lists.newArrayList();

      // figure out which stripes we need to read
      boolean[] includeStripe = null;
      // we can't eliminate stripes if there are deltas because the
      // deltas may change the rows making them match the predicate.
      if (deltas.isEmpty()) {
        Reader.Options options = new Reader.Options();
        options.include(readerIncluded);
        setSearchArgument(options, (readerTypes == null ? fileTypes : readerTypes), context.conf, isOriginal, false);
        // only do split pruning if HIVE-8732 has been fixed in the writer
        if (options.getSearchArgument() != null &&
            writerVersion != OrcFile.WriterVersion.ORIGINAL) {
          // Also, we currently do not use predicate evaluation when the schema has data type
          // conversion.
          if (evolution.hasConversion()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Skipping split elimination for " + file.getPath() +
                  " since the schema has data type conversion");
            }
          } else {
            SearchArgument sarg = options.getSearchArgument();
            List<PredicateLeaf> sargLeaves = sarg.getLeaves();
            int[] filterColumns = RecordReaderImpl.mapSargColumns(sargLeaves,
                evolution);

            if (stripeStats != null) {
              // eliminate stripes that doesn't satisfy the predicate condition
              includeStripe = new boolean[stripes.size()];
              for (int i = 0; i < stripes.size(); ++i) {
                includeStripe[i] = (i >= stripeStats.size()) ||
                    isStripeSatisfyPredicate(stripeStats.get(i), sarg,
                        filterColumns);
                if (LOG.isDebugEnabled() && !includeStripe[i]) {
                  LOG.debug("Eliminating ORC stripe-" + i + " of file '" +
                      file.getPath() + "'  as it did not satisfy " +
                      "predicate condition.");
                }
              }
            }
          }
        }
      }

      // if we didn't have predicate pushdown, read everything
      if (includeStripe == null) {
        includeStripe = new boolean[stripes.size()];
        Arrays.fill(includeStripe, true);
      }

      long currentOffset = -1;
      long currentLength = 0;
      int idx = -1;
      for (StripeInformation stripe : stripes) {
        idx++;

        if (!includeStripe[idx]) {
          // create split for the previous unfinished stripe
          if (currentOffset != -1) {
            splits.add(createSplit(currentOffset, currentLength, orcTail));
            currentOffset = -1;
          }
          continue;
        }

        // if we are working on a stripe, over the min stripe size, and
        // crossed a block boundary, cut the input split here.
        if (currentOffset != -1 && currentLength > context.minSize &&
            (currentOffset / blockSize != stripe.getOffset() / blockSize)) {
          splits.add(createSplit(currentOffset, currentLength, orcTail));
          currentOffset = -1;
        }
        // if we aren't building a split, start a new one.
        if (currentOffset == -1) {
          currentOffset = stripe.getOffset();
          currentLength = stripe.getLength();
        } else {
          currentLength =
              (stripe.getOffset() + stripe.getLength()) - currentOffset;
        }
        if (currentLength >= context.maxSize) {
          splits.add(createSplit(currentOffset, currentLength, orcTail));
          currentOffset = -1;
        }
      }
      if (currentOffset != -1) {
        splits.add(createSplit(currentOffset, currentLength, orcTail));
      }

      // add uncovered ACID delta splits
      splits.addAll(deltaSplits);
      return splits;
    }

    private void populateAndCacheStripeDetails() throws IOException {
      if (orcTail == null) {
        Reader orcReader = OrcFile.createReader(file.getPath(),
                OrcFile.readerOptions(context.conf)
                        .filesystem(fs)
                        .maxLength(file.getLen()));
        orcTail = new OrcTail(orcReader.getFileTail(), orcReader.getSerializedFileFooter(), file.getModificationTime());
        if (context.cacheStripeDetails) {
          context.footerCache.put(file.getPath(), orcTail);
        }
      }

      stripes = orcTail.getStripes();
      stripeStats = orcTail.getStripeStatistics();
      fileTypes = orcTail.getTypes();
      TypeDescription fileSchema = OrcUtils.convertTypeFromProtobuf(fileTypes, 0);
      if (readerTypes == null) {
        readerIncluded = genIncludedColumns(fileSchema, context.conf);
        evolution = new SchemaEvolution(fileSchema, readerIncluded);
      } else {
        TypeDescription readerSchema = OrcUtils.convertTypeFromProtobuf(readerTypes, 0);
        // The readerSchema always comes in without ACID columns.
        readerIncluded = genIncludedColumns(readerSchema, context.conf);
        evolution = new SchemaEvolution(fileSchema, readerSchema, readerIncluded);
        if (!isOriginal) {
          // The SchemaEvolution class has added the ACID metadata columns.  Let's update our
          // readerTypes so PPD code will work correctly.
          readerTypes = OrcUtils.getOrcTypes(evolution.getReaderSchema());
        }
      }
      writerVersion = orcTail.getWriterVersion();
      List<OrcProto.ColumnStatistics> fileColStats = orcTail.getFooter().getStatisticsList();
      boolean[] fileIncluded;
      if (readerTypes == null) {
        fileIncluded = readerIncluded;
      } else {
        fileIncluded = new boolean[fileTypes.size()];
        final int readerSchemaSize = readerTypes.size();
        for (int i = 0; i < readerSchemaSize; i++) {
          TypeDescription fileType = evolution.getFileType(i);
          if (fileType != null) {
            fileIncluded[fileType.getId()] = true;
          }
        }
      }
      projColsUncompressedSize = computeProjectionSize(fileTypes, fileColStats, fileIncluded);
      if (!context.footerInSplits) {
        orcTail = null;
      }
    }

    private long computeProjectionSize(List<OrcProto.Type> fileTypes,
        List<OrcProto.ColumnStatistics> stats, boolean[] fileIncluded) {
      List<Integer> internalColIds = Lists.newArrayList();
      if (fileIncluded == null) {
        // Add all.
        for (int i = 0; i < fileTypes.size(); i++) {
          internalColIds.add(i);
        }
      } else {
        for (int i = 0; i < fileIncluded.length; i++) {
          if (fileIncluded[i]) {
            internalColIds.add(i);
          }
        }
      }
      return ReaderImpl.getRawDataSizeFromColIndices(internalColIds, fileTypes, stats);
    }

    private boolean isStripeSatisfyPredicate(StripeStatistics stripeStatistics,
                                             SearchArgument sarg,
                                             int[] filterColumns) {
      List<PredicateLeaf> predLeaves = sarg.getLeaves();
      TruthValue[] truthValues = new TruthValue[predLeaves.size()];
      for (int pred = 0; pred < truthValues.length; pred++) {
        if (filterColumns[pred] != -1) {

          // column statistics at index 0 contains only the number of rows
          ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
          truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, predLeaves.get(pred), null);
        } else {

          // parition column case.
          // partition filter will be evaluated by partition pruner so
          // we will not evaluate partition filter here.
          truthValues[pred] = TruthValue.YES_NO_NULL;
        }
      }
      return sarg.evaluate(truthValues).isNeeded();
    }
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf)
      throws IOException {
    return generateSplitsInfo(conf, -1);
  }

  static List<OrcSplit> generateSplitsInfo(Configuration conf, int numSplits)
      throws IOException {
    // use threads to resolve directories into splits
    Context context = new Context(conf, numSplits);
    if (LOG.isInfoEnabled()) {
      LOG.info("ORC pushdown predicate: " + SearchArgumentFactory.createFromConf(conf));
    }
    List<OrcSplit> splits = Lists.newArrayList();
    List<Future<?>> pathFutures = Lists.newArrayList();
    List<Future<?>> splitFutures = Lists.newArrayList();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    boolean isTransactionalTableScan =
            HiveConf.getBoolVar(conf, ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN);
    boolean isSchemaEvolution = HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION);
    TypeDescription readerSchema =
        OrcUtils.getDesiredRowTypeDescr(conf, isTransactionalTableScan);
    List<OrcProto.Type> readerTypes = null;
    if (readerSchema != null) {
      readerTypes = OrcUtils.getOrcTypes(readerSchema);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generate splits schema evolution property " + isSchemaEvolution +
        " reader schema " + (readerSchema == null ? "NULL" : readerSchema.toString()) +
        " transactional scan property " + isTransactionalTableScan);
    }

    // multi-threaded file statuses and split strategy
    for (Path dir : getInputPaths(conf)) {
      FileSystem fs = dir.getFileSystem(conf);
      FileGenerator fileGenerator = new FileGenerator(context, fs, dir, readerTypes, ugi);
      pathFutures.add(context.threadPool.submit(fileGenerator));
    }

    // complete path futures and schedule split generation
    try {
      for (Future<?> pathFuture : pathFutures) {
        SplitStrategy splitStrategy = (SplitStrategy) pathFuture.get();

        if (isDebugEnabled) {
          LOG.debug(splitStrategy);
        }

        if (splitStrategy instanceof ETLSplitStrategy) {
          List<SplitInfo> splitInfos = splitStrategy.getSplits();
          for (SplitInfo splitInfo : splitInfos) {
            splitFutures.add(context.threadPool.submit(new SplitGenerator(splitInfo, ugi)));
          }
        } else {
          splits.addAll(splitStrategy.getSplits());
        }
      }

      // complete split futures
      for (Future<?> splitFuture : splitFutures) {
        splits.addAll((Collection<? extends OrcSplit>) splitFuture.get());
      }
    } catch (Exception e) {
      cancelFutures(pathFutures);
      cancelFutures(splitFutures);
      throw new RuntimeException("serious problem", e);
    }

    if (context.cacheStripeDetails) {
      LOG.info("FooterCacheHitRatio: " + context.cacheHitCounter.get() + "/"
          + context.numFilesCounter.get());
    }

    if (isDebugEnabled) {
      for (OrcSplit split : splits) {
        LOG.debug(split + " projected_columns_uncompressed_size: "
            + split.getColumnarProjectionSize());
      }
    }
    return splits;
  }

  private static void cancelFutures(List<Future<?>> futures) {
    for (Future future : futures) {
      future.cancel(true);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job,
                                int numSplits) throws IOException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
    List<OrcSplit> result = generateSplitsInfo(job, numSplits);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
    return result.toArray(new InputSplit[result.size()]);
  }

  @SuppressWarnings("unchecked")
  private org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
    createVectorizedReader(InputSplit split, JobConf conf, Reporter reporter
                           ) throws IOException {
    return (org.apache.hadoop.mapred.RecordReader)
      new VectorizedOrcInputFormat().getRecordReader(split, conf, reporter);
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
  getRecordReader(InputSplit inputSplit, JobConf conf,
                  Reporter reporter) throws IOException {
    boolean vectorMode = Utilities.isVectorMode(conf);
    boolean isAcidRead = isAcidRead(conf, inputSplit);

    if (!isAcidRead) {
      if (vectorMode) {
        return createVectorizedReader(inputSplit, conf, reporter);
      } else {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        if (inputSplit instanceof OrcSplit) {
          OrcSplit split = (OrcSplit) inputSplit;
          readerOptions.maxLength(split.getFileLength()).orcTail(split.getOrcTail());
        }
        return new OrcRecordReader(OrcFile.createReader(
            ((FileSplit) inputSplit).getPath(),
            readerOptions), conf, (FileSplit) inputSplit);
      }
    }

    reporter.setStatus(inputSplit.toString());

    Options options = new Options(conf).reporter(reporter);
    final RowReader<OrcStruct> inner = getReader(inputSplit, options);

    if (vectorMode) {
      return (org.apache.hadoop.mapred.RecordReader)
          new VectorizedOrcAcidRowReader(inner, conf,
              Utilities.getMapWork(conf).getVectorizedRowBatchCtx(), (FileSplit) inputSplit);
    } else {
      return new NullKeyRecordReader(inner, conf);
    }
  }
  /**
   * Return a RecordReader that is compatible with the Hive 0.12 reader
   * with NullWritable for the key instead of RecordIdentifier.
   */
  public static final class NullKeyRecordReader implements AcidRecordReader<NullWritable, OrcStruct> {
    private final RecordIdentifier id;
    private final RowReader<OrcStruct> inner;

    public RecordIdentifier getRecordIdentifier() {
      return id;
    }
    private NullKeyRecordReader(RowReader<OrcStruct> inner, Configuration conf) {
      this.inner = inner;
      id = inner.createKey();
    }
    @Override
    public boolean next(NullWritable nullWritable,
                        OrcStruct orcStruct) throws IOException {
      return inner.next(id, orcStruct);
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return inner.createValue();
    }

    @Override
    public long getPos() throws IOException {
      return inner.getPos();
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }

    @Override
    public float getProgress() throws IOException {
      return inner.getProgress();
    }
  }

  // The schema type description does not include the ACID fields (i.e. it is the
  // non-ACID original schema).
  private static boolean SCHEMA_TYPES_IS_ORIGINAL = true;

  @Override
  public RowReader<OrcStruct> getReader(InputSplit inputSplit,
                                        Options options)
                                            throws IOException {

    final OrcSplit split = (OrcSplit) inputSplit;
    final Path path = split.getPath();
    Path root;
    if (split.hasBase()) {
      if (split.isOriginal()) {
        root = path.getParent();
      } else {
        root = path.getParent().getParent();
      }
    } else {
      root = path;
    }
    final Path[] deltas = AcidUtils.deserializeDeltas(root, split.getDeltas());
    final Configuration conf = options.getConfiguration();


    /**
     * Do we have schema on read in the configuration variables?
     */
    TypeDescription schema = OrcUtils.getDesiredRowTypeDescr(conf, /* isAcidRead */ true);

    final Reader reader;
    final int bucket;
    Reader.Options readOptions = new Reader.Options().schema(schema);
    readOptions.range(split.getStart(), split.getLength());

    // TODO: Convert genIncludedColumns and setSearchArgument to use TypeDescription.
    final List<Type> schemaTypes = OrcUtils.getOrcTypes(schema);
    readOptions.include(genIncludedColumns(schema, conf));
    setSearchArgument(readOptions, schemaTypes, conf, SCHEMA_TYPES_IS_ORIGINAL);

    if (split.hasBase()) {
      bucket = AcidUtils.parseBaseBucketFilename(split.getPath(), conf)
          .getBucket();
      OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
              .maxLength(split.getFileLength());
      if (split.hasFooter()) {
        readerOptions.orcTail(split.getOrcTail());
      }
      reader = OrcFile.createReader(path, readerOptions);
    } else {
      bucket = (int) split.getStart();
      reader = null;
    }
    String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY);
    ValidTxnList validTxnList = txnString == null ? new ValidReadTxnList() :
      new ValidReadTxnList(txnString);
    final OrcRawRecordMerger records =
        new OrcRawRecordMerger(conf, true, reader, split.isOriginal(), bucket,
            validTxnList, readOptions, deltas);
    return new RowReader<OrcStruct>() {
      OrcStruct innerRecord = records.createValue();

      @Override
      public ObjectInspector getObjectInspector() {
        return OrcStruct.createObjectInspector(0, schemaTypes);
      }

      @Override
      public boolean next(RecordIdentifier recordIdentifier,
                          OrcStruct orcStruct) throws IOException {
        boolean result;
        // filter out the deleted records
        do {
          result = records.next(recordIdentifier, innerRecord);
        } while (result &&
            OrcRecordUpdater.getOperation(innerRecord) ==
                OrcRecordUpdater.DELETE_OPERATION);
        if (result) {
          // swap the fields with the passed in orcStruct
          orcStruct.linkFields(OrcRecordUpdater.getRow(innerRecord));
        }
        return result;
      }

      @Override
      public RecordIdentifier createKey() {
        return records.createKey();
      }

      @Override
      public OrcStruct createValue() {
        return new OrcStruct(records.getColumns());
      }

      @Override
      public long getPos() throws IOException {
        return records.getPos();
      }

      @Override
      public void close() throws IOException {
        records.close();
      }

      @Override
      public float getProgress() throws IOException {
        return records.getProgress();
      }
    };
  }

  static Path findOriginalBucket(FileSystem fs,
                                 Path directory,
                                 int bucket) throws IOException {
    for(FileStatus stat: fs.listStatus(directory)) {
      String name = stat.getPath().getName();
      String numberPart = name.substring(0, name.indexOf('_'));
      if (org.apache.commons.lang3.StringUtils.isNumeric(numberPart) &&
          Integer.parseInt(numberPart) == bucket) {
        return stat.getPath();
      }
    }
    throw new IllegalArgumentException("Can't find bucket " + bucket + " in " +
        directory);
  }

  @Override
  public RawReader<OrcStruct> getRawReader(Configuration conf,
                                           boolean collapseEvents,
                                           int bucket,
                                           ValidTxnList validTxnList,
                                           Path baseDirectory,
                                           Path[] deltaDirectory
                                           ) throws IOException {
    Reader reader = null;
    boolean isOriginal = false;
    if (baseDirectory != null) {
      Path bucketFile;
      if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX)) {
        bucketFile = AcidUtils.createBucketFile(baseDirectory, bucket);
      } else {
        isOriginal = true;
        bucketFile = findOriginalBucket(baseDirectory.getFileSystem(conf),
            baseDirectory, bucket);
      }
      reader = OrcFile.createReader(bucketFile, OrcFile.readerOptions(conf));
    }
    return new OrcRawRecordMerger(conf, collapseEvents, reader, isOriginal,
        bucket, validTxnList, new Reader.Options(), deltaDirectory);
  }

}
