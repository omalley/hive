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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.ql.io.FileFormatException;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.UserMetadataItem;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.CodedInputStream;

public class ReaderImpl implements Reader {

  private static final Log LOG = LogFactory.getLog(ReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;
  private static final int DEFAULT_PROTOBUF_MESSAGE_LIMIT = 64 << 20;  // 64MB
  private static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20; // 1GB

  protected final FileSystem fileSystem;
  protected final Path path;
  protected final CompressionKind compressionKind;
  protected final CompressionCodec codec;
  protected final int bufferSize;
  private OrcProto.Metadata metadata = null;
  private final int metadataSize;
  private final TypeDescription schema;
  protected final OrcProto.Footer footer;
  private final ObjectInspector inspector;
  private long deserializedSize = -1;
  protected final Configuration conf;
  private final List<Integer> versionList;
  private final OrcFile.WriterVersion writerVersion;

  //serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private final ByteBuffer footerByteBuffer;
  private final OrcTail tail;

  static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getDataLength() + getIndexLength() + getFooterLength();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " + getDataLength() +
        " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
        " index: " + getIndexLength();
    }
  }

  @Override
  public long getNumberOfRows() {
    return footer.getNumberOfRows();
  }

  @Override
  public List<String> getMetadataKeys() {
    List<String> result = new ArrayList<String>();
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  public boolean hasMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public CompressionKind getCompression() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public List<StripeInformation> getStripes() {
    List<StripeInformation> result = new ArrayList<StripeInformation>();
    for(OrcProto.StripeInformation info: footer.getStripesList()) {
      result.add(new StripeInformationImpl(info));
    }
    return result;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public long getContentLength() {
    return footer.getContentLength();
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return footer.getTypesList();
  }

  @Override
  public OrcFile.Version getFileVersion() {
    for (OrcFile.Version version: OrcFile.Version.values()) {
      if (version.getMajor() == versionList.get(0) &&
          version.getMinor() == versionList.get(1)) {
        return version;
      }
    }
    return OrcFile.Version.V_0_11;
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }

  @Override
  public int getRowIndexStride() {
    return footer.getRowIndexStride();
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[footer.getTypesCount()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl.deserialize(footer.getStatistics(i));
    }
    return result;
  }

  @Override
  public TypeDescription getSchema() {
    return schema;
  }

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param in the file being read
   * @param path the filename for error messages
   * @param psLen the postscript length
   * @param buffer the tail of the file
   * @throws IOException
   */
  static void ensureOrcFooter(FSDataInputStream in,
                                      Path path,
                                      int psLen,
                                      ByteBuffer buffer) throws IOException {
    int magicLength = OrcFile.MAGIC.length();
    int fullLength = magicLength + 1;
    if (psLen < fullLength || buffer.remaining() < fullLength) {
      throw new FileFormatException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - fullLength;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      byte[] header = new byte[magicLength];
      in.readFully(0, header, 0, magicLength);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , magicLength).equals(OrcFile.MAGIC)) {
        throw new FileFormatException("Malformed ORC file " + path +
            ". Invalid postscript.");
      }
    }
  }

  /**
   * Build a version string out of an array.
   * @param version the version number as a list
   * @return the human readable form of the version string
   */
  private static String versionString(List<Integer> version) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < version.size(); ++i) {
      if (i != 0) {
        buffer.append('.');
      }
      buffer.append(version.get(i));
    }
    return buffer.toString();
  }

  /**
   * Check to see if this ORC file is from a future version and if so,
   * warn the user that we may not be able to read all of the column encodings.
   * @param log the logger to write any error message to
   * @param path the filename for error messages
   * @param version the version of hive that wrote the file.
   */
  static void checkOrcVersion(Log log, Path path, List<Integer> version) {
    if (version.size() >= 1) {
      int major = version.get(0);
      int minor = 0;
      if (version.size() >= 2) {
        minor = version.get(1);
      }
      if (major > OrcFile.Version.CURRENT.getMajor() ||
          (major == OrcFile.Version.CURRENT.getMajor() &&
           minor > OrcFile.Version.CURRENT.getMinor())) {
        log.warn("ORC file " + path +
                 " was written by a future Hive version " +
                 versionString(version) +
                 ". This file may not be readable by this version of Hive.");
      }
    }
  }

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   * @throws IOException
   */
  public ReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = path.getFileSystem(options.getConfiguration());
    }
    this.fileSystem = fs;
    this.path = path;
    this.conf = options.getConfiguration();

    OrcTail orcTail = options.getOrcTail();
    if (orcTail == null) {
      tail = extractFileTail(fs, path, options.getMaxLength());
      options.orcTail(tail);
    } else {
      tail = orcTail;
    }
    this.footerByteBuffer = tail.getSerializedTail();
    this.compressionKind = tail.getCompressionKind();
    this.codec = tail.getCompressionCodec();
    this.bufferSize = tail.getCompressionBufferSize();
    this.metadataSize = tail.getMetadataSize();
    this.versionList = tail.getPostScript().getVersionList();
    this.footer = tail.getFooter();
    this.writerVersion = tail.getWriterVersion();
    this.metadata = tail.getMetadata();
    this.inspector = OrcStruct.createObjectInspector(0, tail.getTypes());
    this.schema = OrcUtils.convertTypeFromProtobuf(tail.getTypes(), 0);
  }

  /**
   * Get the WriterVersion based on the ORC file postscript.
   * @param writerVersion the integer writer version
   * @return
   */
  static OrcFile.WriterVersion getWriterVersion(int writerVersion) {
    for(OrcFile.WriterVersion version: OrcFile.WriterVersion.values()) {
      if (version.getId() == writerVersion) {
        return version;
      }
    }
    return OrcFile.WriterVersion.ORIGINAL;
  }

  private static OrcTail extractFileTail(FileSystem fs,
                                                        Path path,
                                                        long maxFileLength
                                                        ) throws IOException {
    FSDataInputStream file = fs.open(path);

    OrcProto.FileTail.Builder fileTailBuilder = OrcProto.FileTail.newBuilder();
    // figure out the size of the file using the option or filesystem
    long size;
    long modificationTime;
    ByteBuffer buffer;
    try {
      if (maxFileLength == Long.MAX_VALUE) {
        FileStatus fileStatus = fs.getFileStatus(path);
        size = fileStatus.getLen();
        modificationTime = fileStatus.getModificationTime();
      } else {
        size = maxFileLength;
        modificationTime = -1;
      }
      fileTailBuilder.setFileLength(size);

      //read last bytes into buffer to get PostScript
      int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
      buffer = ByteBuffer.allocate(readSize);
      file.readFully((size - readSize),
          buffer.array(), buffer.arrayOffset(), readSize);

      //read the PostScript
      //get length of PostScript
      int psLen = buffer.get(readSize - 1) & 0xff;
      ensureOrcFooter(file, path, psLen, buffer);
      int psOffset = readSize - 1 - psLen;
      CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
              buffer.arrayOffset() + psOffset, psLen);
      OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);

      checkOrcVersion(LOG, path, ps.getVersionList());

      int footerSize = (int) ps.getFooterLength();
      int metadataSize = (int) ps.getMetadataLength();

      //check compression codec
      switch (ps.getCompression()) {
        case NONE:
          break;
        case ZLIB:
          break;
        case SNAPPY:
          break;
        case LZO:
          break;
        default:
          throw new IllegalArgumentException("Unknown compression");
      }

      //check if extra bytes need to be read
      int extra = Math.max(0, psLen + 1 + footerSize + metadataSize - readSize);
      if (extra > 0) {
        //more bytes need to be read, seek back to the right place and read extra bytes
        ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
        file.readFully((size - readSize - extra), extraBuf.array(),
            extraBuf.arrayOffset() + extraBuf.position(), extra);
        extraBuf.position(extra);
        //append with already read bytes
        extraBuf.put(buffer);
        buffer = extraBuf;
        buffer.position(0);
        buffer.limit(footerSize + metadataSize);
        readSize += extra;
        psOffset = readSize - 1 - psLen;
      } else {
        //footer is already in the bytes in buffer, just adjust position, length
        buffer.position(psOffset - footerSize - metadataSize);
        buffer.limit(psOffset);
      }

      // remember position for later
      buffer.mark();

      int bufferSize = (int) ps.getCompressionBlockSize();
      CompressionCodec codec = WriterImpl.createCodec(CompressionKind.valueOf(ps.getCompression().name()));
      fileTailBuilder.setPostscriptLength(psLen).setPostscript(ps);
      int footerOffset = psOffset - footerSize;
      buffer.position(footerOffset);
      ByteBuffer footerBuffer = buffer.slice();
      buffer.reset();
      OrcProto.Footer footer = extractFooter(footerBuffer, 0, footerSize,
              codec, bufferSize);
      fileTailBuilder.setFooter(footer);
    } finally {
      try {
        file.close();
      } catch (IOException ex) {
        LOG.error("Failed to close the file after another error", ex);
      }
    }

    return new OrcTail(fileTailBuilder.build(), buffer.slice(), modificationTime);
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
                                               int footerSize, CompressionCodec codec,
                                               int bufferSize) throws IOException {
    bb.position(footerAbsPos);
    bb.limit(footerAbsPos + footerSize);
    CodedInputStream in = InStream.createCodedInputStream("footer",
            Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize);
    return OrcProto.Footer.parseFrom(in);
  }

  public static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
                                                  int metadataSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    CodedInputStream in = InStream.createCodedInputStream("metadata",
            Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), metadataSize, codec, bufferSize);
    return OrcProto.Metadata.parseFrom(in);
  }

  @Override
  public RecordReader rows() throws IOException {
    return rowsOptions(new Options());
  }

  @Override
  public RecordReader rowsOptions(Options options) throws IOException {
    LOG.info("Reading ORC rows from " + path + " with " + options);
    return new RecordReaderImpl(this, options);
  }


  @Override
  public RecordReader rows(boolean[] include) throws IOException {
    return rowsOptions(new Options().include(include));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length));
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include,
                           SearchArgument sarg, String[] columnNames
                           ) throws IOException {
    return rowsOptions(new Options().include(include).range(offset, length)
        .searchArgument(sarg, columnNames));
  }

  @Override
  public long getRawDataSize() {
    // if the deserializedSize is not computed, then compute it, else
    // return the already computed size. since we are reading from the footer
    // we don't have to compute deserialized size repeatedly
    if (deserializedSize == -1) {
      List<OrcProto.ColumnStatistics> stats = footer.getStatisticsList();
      List<Integer> indices = Lists.newArrayList();
      for (int i = 0; i < stats.size(); ++i) {
        indices.add(i);
      }
      deserializedSize = getRawDataSizeFromColIndices(indices, footer.getTypesList(), stats);
    }
    return deserializedSize;
  }

  @Override
  public OrcProto.FileTail getFileTail() {
    return tail.getFileTail();
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return tail.getSerializedTail();
  }

  public static long getRawDataSizeFromColIndices(List<Integer> colIndices, List<Type> types,
                                                  List<OrcProto.ColumnStatistics> stats) {
    long result = 0;
    for (int colIdx : colIndices) {
      result += getRawDataSizeOfColumn(colIdx, types, stats);
    }
    return result;
  }

  private static long getRawDataSizeOfColumn(int colIdx, List<Type> types, List<OrcProto.ColumnStatistics> stats) {
    OrcProto.ColumnStatistics colStat = stats.get(colIdx);
    long numVals = colStat.getNumberOfValues();
    Type type = types.get(colIdx);

    switch (type.getKind()) {
    case BINARY:
      // old orc format doesn't support binary statistics. checking for binary
      // statistics is not required as protocol buffers takes care of it.
      return colStat.getBinaryStatistics().getSum();
    case STRING:
    case CHAR:
    case VARCHAR:
      // old orc format doesn't support sum for string statistics. checking for
      // existence is not required as protocol buffers takes care of it.

      // ORC strings are deserialized to java strings. so use java data model's
      // string size
      numVals = numVals == 0 ? 1 : numVals;
      int avgStrLen = (int) (colStat.getStringStatistics().getSum() / numVals);
      return numVals * JavaDataModel.get().lengthForStringOfLength(avgStrLen);
    case TIMESTAMP:
      return numVals * JavaDataModel.get().lengthOfTimestamp();
    case DATE:
      return numVals * JavaDataModel.get().lengthOfDate();
    case DECIMAL:
      return numVals * JavaDataModel.get().lengthOfDecimal();
    case DOUBLE:
    case LONG:
      return numVals * JavaDataModel.get().primitive2();
    case FLOAT:
    case INT:
    case SHORT:
    case BOOLEAN:
    case BYTE:
      return numVals * JavaDataModel.get().primitive1();
    default:
      LOG.debug("Unknown primitive category: " + type.getKind());
      break;
    }

    return 0;
  }

  @Override
  public long getRawDataSizeOfColumns(List<String> colNames) {
    List<Integer> colIndices = getColumnIndicesFromNames(colNames);
    return getRawDataSizeFromColIndices(colIndices, tail.getTypes(), tail.getFooter().getStatisticsList());
  }

  @Override
  public long getRawDataSizeFromColIndices(List<Integer> colIds) {
    return getRawDataSizeFromColIndices(colIds, tail.getTypes(), tail.getFooter().getStatisticsList());
  }

  private List<Integer> getColumnIndicesFromNames(List<String> colNames) {
    // top level struct
    Type type = footer.getTypesList().get(0);
    List<Integer> colIndices = Lists.newArrayList();
    List<String> fieldNames = type.getFieldNamesList();
    int fieldIdx = 0;
    for (String colName : colNames) {
      if (fieldNames.contains(colName)) {
        fieldIdx = fieldNames.indexOf(colName);
      } else {
        String s = "Cannot find field for: " + colName + " in ";
        for (String fn : fieldNames) {
          s += fn + ", ";
        }
        LOG.warn(s);
        continue;
      }

      // a single field may span multiple columns. find start and end column
      // index for the requested field
      int idxStart = type.getSubtypes(fieldIdx);

      int idxEnd;

      // if the specified is the last field and then end index will be last
      // column index
      if (fieldIdx + 1 > fieldNames.size() - 1) {
        idxEnd = getLastIdx() + 1;
      } else {
        idxEnd = type.getSubtypes(fieldIdx + 1);
      }

      // if start index and end index are same then the field is a primitive
      // field else complex field (like map, list, struct, union)
      if (idxStart == idxEnd) {
        // simple field
        colIndices.add(idxStart);
      } else {
        // complex fields spans multiple columns
        for (int i = idxStart; i < idxEnd; i++) {
          colIndices.add(i);
        }
      }
    }
    return colIndices;
  }

  private int getLastIdx() {
    Set<Integer> indices = Sets.newHashSet();
    for (Type type : footer.getTypesList()) {
      indices.addAll(type.getSubtypesList());
    }
    return Collections.max(indices);
  }

  @Override
  public Metadata getMetadata() throws IOException {
    return new Metadata(metadata);
  }

  List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
    return metadata.getStripeStatsList();
  }

  public List<UserMetadataItem> getOrcProtoUserMetadata() {
    return footer.getMetadataList();
  }

  @Override
  public MetadataReader metadata() throws IOException {
    return new MetadataReader(fileSystem, path, codec, bufferSize, footer.getTypesCount());
  }
}
