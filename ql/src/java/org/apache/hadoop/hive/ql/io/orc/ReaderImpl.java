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
import java.util.Arrays;
import java.util.List;

import org.apache.orc.impl.BufferChunk;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.MetadataReader;
import org.apache.orc.impl.MetadataReaderImpl;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.io.FileFormatException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcProto;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;

public class ReaderImpl implements Reader {

  private static final Logger LOG = LoggerFactory.getLogger(ReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  protected final FileSystem fileSystem;
  private final long maxLength;
  protected final Path path;
  protected final CompressionKind compressionKind;
  protected final CompressionCodec codec;
  protected final int bufferSize;
  private final List<StripeInformation> stripes;
  private final OrcProto.FileTail fileTail;
  private OrcProto.Metadata stripeStatistics;
  private final OrcFile.WriterVersion writerVersion;

  private long deserializedSize = -1;
  protected final Configuration conf;

  public static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    public StripeInformationImpl(OrcProto.StripeInformation stripe) {
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
    return fileTail.getFooter().getNumberOfRows();
  }

  @Override
  public List<String> getMetadataKeys() {
    List<String> result = new ArrayList<String>();
    for(OrcProto.UserMetadataItem item:
        fileTail.getFooter().getMetadataList()) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: fileTail.getFooter().getMetadataList()){
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  public boolean hasMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: fileTail.getFooter().getMetadataList()){
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
  public org.apache.orc.CompressionKind getCompressionKind() {
    return compressionKind.getUnderlying();
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return stripes;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return OrcStruct.createObjectInspector(0,
        fileTail.getFooter().getTypesList());
  }

  @Override
  public long getContentLength() {
    return fileTail.getFooter().getContentLength();
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return fileTail.getFooter().getTypesList();
  }

  @Override
  public OrcFile.Version getFileVersion() {
    List<Integer> versionList = fileTail.getPostscript().getVersionList();
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
    return fileTail.getFooter().getRowIndexStride();
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result =
        new ColumnStatistics[fileTail.getFooter().getTypesList().size()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl.deserialize
          (fileTail.getFooter().getStatistics(i));
    }
    return result;
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
    int len = OrcFile.MAGIC.length();
    if (psLen < len + 1) {
      throw new FileFormatException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      byte[] header = new byte[len];
      in.readFully(0, header, 0, len);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , len).equals(OrcFile.MAGIC)) {
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
   * @param path the data source path for error messages
   * @param version the version of hive that wrote the file.
   */
  static void checkOrcVersion(Logger log, Path path, List<Integer> version) {
    if (version.size() >= 1) {
      int major = version.get(0);
      int minor = 0;
      if (version.size() >= 2) {
        minor = version.get(1);
      }
      if (major > OrcFile.Version.CURRENT.getMajor() ||
          (major == OrcFile.Version.CURRENT.getMajor() &&
           minor > OrcFile.Version.CURRENT.getMinor())) {
        log.warn(path + " was written by a future Hive version " +
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
    this.maxLength = options.getMaxLength();

    ByteBuffer tailBuffer = options.getFileTail();
    if (tailBuffer == null) {
      this.fileTail = parseFooter(fs, path, options.getMaxLength());
    } else {
      int posn = tailBuffer.position();
      int length = tailBuffer.limit() - posn;
      this.fileTail = OrcProto.FileTail.parseFrom(
          CodedInputStream.newInstance(tailBuffer.array(),
              tailBuffer.arrayOffset() + posn, length));
    }
    if (fileTail.hasMetadata()) {
      this.stripeStatistics = fileTail.getMetadata();
    }
    this.compressionKind =
        CompressionKind.byId(fileTail.getPostscript().getCompression()
            .getNumber());
    this.codec = WriterImpl.createCodec(compressionKind.getUnderlying());
    this.stripes = convertProtoStripesToStripes(fileTail.getFooter().getStripesList());
    this.writerVersion = extractWriterVersion(fileTail.getPostscript());
    this.bufferSize = (int) fileTail.getPostscript().getCompressionBlockSize();
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
      int footerSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(footerAbsPos);
    bb.limit(footerAbsPos + footerSize);
    return OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer",
        Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize));
  }

  private static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
      int metadataSize, CompressionCodec codec, int bufferSize) throws IOException {
    bb.position(metadataAbsPos);
    bb.limit(metadataAbsPos + metadataSize);
    return OrcProto.Metadata.parseFrom(InStream.createCodedInputStream("metadata",
        Lists.<DiskRange>newArrayList(new BufferChunk(bb, 0)), metadataSize, codec, bufferSize));
  }

  private static OrcProto.PostScript extractPostScript(ByteBuffer bb, Path path,
      int psLen, int psAbsOffset) throws IOException {
    // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
    assert bb.hasArray();
    CodedInputStream in = CodedInputStream.newInstance(
        bb.array(), bb.arrayOffset() + psAbsOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    checkOrcVersion(LOG, path, ps.getVersionList());

    // Check compression codec.
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
        throw new IllegalArgumentException("Unknown compression in " + path);
    }
    return ps;
  }

  /**
   * Read the file footer from a file.
   * Also sets the compressionKind, codec, and stripeStatistics fields.
   * @param fs the filesystem to read the file from
   * @param path the filename to read
   * @param maxFileLength the position in the file to treat as the end of the
   *                      file or -1 if the real file length should be used.
   * @return the parsed file tail
   * @throws IOException
   */
  private OrcProto.FileTail parseFooter(FileSystem fs,
                                        Path path,
                                        long maxFileLength
                                        ) throws IOException {
    OrcProto.FileTail.Builder result = OrcProto.FileTail.newBuilder();
    FSDataInputStream file = fs.open(path);

    // figure out the size of the file using the option or filesystem
    long size;
    if (maxFileLength == Long.MAX_VALUE) {
      size = fs.getFileStatus(path).getLen();
    } else {
      size = maxFileLength;
    }

    //read last bytes into buffer to get PostScript
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    assert buffer.position() == 0;
    file.readFully((size - readSize),
        buffer.array(), buffer.arrayOffset(), readSize);

    //read the PostScript
    //get length of PostScript
    int psLen = buffer.get(readSize - 1) & 0xff;
    ensureOrcFooter(file, path, psLen, buffer);
    int psOffset = readSize - 1 - psLen;
    OrcProto.PostScript ps = extractPostScript(buffer, path, psLen, psOffset);
    result.setPostscript(ps);

    int footerSize = (int) ps.getFooterLength();
    result.setFileLength(size);
    result.setPostscriptLength(psLen);
    int metadataSize = (int) ps.getMetadataLength();
    int footerOffset;
    int metadataOffset;

    // Do we have the entire file footer & metadata?
    // Just use it!
    int wholeTailSize = 1 + psLen + footerSize + metadataSize;
    if (wholeTailSize <= readSize) {
      metadataOffset = readSize - wholeTailSize;
      footerOffset = metadataOffset + metadataSize;

    // Do we have the footer, but not the metadata?
    // Use the footer and load the metadata lazily.
    } else if (wholeTailSize - metadataSize <= readSize) {
      footerOffset = readSize - wholeTailSize + metadataSize;
      metadataOffset = -1;

    // We don't even have the footer. Load it all.
    } else {
      // more bytes need to be read, seek back to the right place and read
      // extra bytes
      ByteBuffer extraBuf = ByteBuffer.allocate(wholeTailSize);
      file.readFully((size - wholeTailSize), extraBuf.array(),
          extraBuf.arrayOffset() + extraBuf.position(),
          wholeTailSize - readSize);
      extraBuf.position(wholeTailSize - readSize);
      //append with already read bytes
      extraBuf.put(buffer);
      buffer = extraBuf;
      buffer.position(0);
      buffer.limit(footerSize + metadataSize);
      metadataOffset = 0;
      footerOffset = metadataSize;
    }
    file.close();

    CompressionKind compressionKind =
        CompressionKind.byId(ps.getCompression().getNumber());
    int bufferSize = (int) ps.getCompressionBlockSize();
    CompressionCodec codec =
        WriterImpl.createCodec(compressionKind.getUnderlying());
    result.setFooter(extractFooter(buffer, footerOffset, footerSize,
        codec, bufferSize));
    if (metadataOffset != -1) {
      result.setMetadata(extractMetadata(buffer, metadataOffset, metadataSize,
          codec, bufferSize));
    }

    return result.build();
  }

  private static OrcFile.WriterVersion extractWriterVersion(OrcProto.PostScript ps) {
    return (ps.hasWriterVersion()
        ? OrcFile.WriterVersion.from(ps.getWriterVersion())
        : OrcFile.WriterVersion.ORIGINAL);
  }

  private static List<StripeInformation> convertProtoStripesToStripes(
      List<OrcProto.StripeInformation> stripes) {
    List<StripeInformation> result = new ArrayList<StripeInformation>(stripes.size());
    for (OrcProto.StripeInformation info : stripes) {
      result.add(new StripeInformationImpl(info));
    }
    return result;
  }

  @Override
  public ByteBuffer getSerializedFileFooter(boolean includeStripeStats) throws IOException {
    if (includeStripeStats != fileTail.hasMetadata()) {
      if (includeStripeStats) {
        // force the loading of the Metadata
        getOrcProtoStripeStatistics();
        // serialize the whole blob
        return ByteBuffer.wrap(fileTail.toBuilder()
            .setMetadata(stripeStatistics).build().toByteArray());
      } else {
        // clear the metadata from the fileTail
        return ByteBuffer.wrap(fileTail.toBuilder()
            .clearMetadata().build().toByteArray());
      }
    } else {
      // just serialize the object we have
      return ByteBuffer.wrap(fileTail.toByteArray());
    }
  }

  @Override
  public RecordReader rows() throws IOException {
    return rowsOptions(new Options());
  }

  @Override
  public RecordReader rowsOptions(Options options) throws IOException {
    LOG.info("Reading ORC rows from " + path + " with " + options);
    boolean[] include = options.getInclude();
    // if included columns is null, then include all columns
    if (include == null) {
      include = new boolean[fileTail.getFooter().getTypesCount()];
      Arrays.fill(include, true);
      options.include(include);
    }
    return new RecordReaderImpl(this.getStripes(), fileSystem, path,
        options, fileTail.getFooter().getTypesList(), codec, bufferSize,
        fileTail.getFooter().getRowIndexStride(), conf);
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
      deserializedSize = 0;
      for (int i = 0; i < fileTail.getFooter().getStatisticsCount(); ++i) {
        deserializedSize += getRawDataSizeOfColumn(i,
            fileTail.getFooter().getTypesList(),
            fileTail.getFooter().getStatisticsList());
      }
    }
    return deserializedSize;
  }

  @Override
  public long getRawDataSizeFromColumn(int colIndice) {
    return getRawDataSizeOfColumn(colIndice,
        fileTail.getFooter().getTypesList(),
        fileTail.getFooter().getStatisticsList());
  }

  private static long getRawDataSizeOfColumn(int colIdx,
                                             List<OrcProto.Type> types,
      List<OrcProto.ColumnStatistics> stats) {
    OrcProto.ColumnStatistics colStat = stats.get(colIdx);
    long numVals = colStat.getNumberOfValues();
    OrcProto.Type type = types.get(colIdx);

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
    long result = 0;
    for(int c: getColumnIndicesFromNames(colNames)) {
      result += getRawDataSizeFromColumn(c);
    }
    return result;
  }

  private List<Integer> getColumnIndicesFromNames(List<String> colNames) {
    // top level struct
    OrcProto.Type type = fileTail.getFooter().getTypes(0);
    List<Integer> colIndices = Lists.newArrayList();
    List<String> fieldNames = type.getFieldNamesList();
    int fieldIdx;
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
        idxEnd = fileTail.getFooter().getTypesCount();
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

  List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() throws IOException {
    if (stripeStatistics == null) {
      FSDataInputStream in = fileSystem.open(path);
      int metadataLength = (int) fileTail.getPostscript().getMetadataLength();
      ByteBuffer buffer = ByteBuffer.allocate(metadataLength);
      in.read(fileTail.getFileLength() - 1 - fileTail.getPostscriptLength()
              - fileTail.getPostscript().getFooterLength() - metadataLength,
          buffer.array(), buffer.arrayOffset(), metadataLength);
      extractMetadata(buffer, 0, metadataLength, codec, bufferSize);
    }
    return stripeStatistics.getStripeStatsList();
  }

  @Override
  public List<StripeStatistics> getStripeStatistics() throws IOException {
    List<StripeStatistics> result = Lists.newArrayList();
    for (OrcProto.StripeStatistics ss: getOrcProtoStripeStatistics()) {
      result.add(new StripeStatistics(ss.getColStatsList()));
    }
    return result;
  }

  List<OrcProto.UserMetadataItem> getOrcProtoUserMetadata() {
     return fileTail.getFooter().getMetadataList();
  }

  MetadataReader metadata() throws IOException {
    return new MetadataReaderImpl(fileSystem, path, codec, bufferSize,
        fileTail.getFooter().getTypesCount());
  }

  @Override
  public List<Integer> getVersionList() {
    return fileTail.getPostscript().getVersionList();
  }

  @Override
  public int getMetadataSize() {
    return (int) fileTail.getPostscript().getMetadataLength();
  }

  @Override
  public DataReader createDefaultDataReader(boolean useZeroCopy) {
    return RecordReaderUtils.createDefaultDataReader(fileSystem, path, useZeroCopy, codec);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ORC Reader(");
    buffer.append(path);
    if (maxLength != -1) {
      buffer.append(", ");
      buffer.append(maxLength);
    }
    buffer.append(")");
    return buffer.toString();
  }
}
