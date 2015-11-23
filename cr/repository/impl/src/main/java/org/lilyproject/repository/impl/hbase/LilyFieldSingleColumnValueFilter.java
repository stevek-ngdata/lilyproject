// LILY SPECIFIC NOTES:
//
//   This class has been copied from HBase and adjusted to deal with metadata and field flags.
//   Rather then writing an entirely new filter specific for Lily field values, we've kept it
//   as close to the original as possible. In the future it might make sense to do something
//   completely different.
//
//   The parts changed for Lily are marked with "Lily change"
//
/**
 * This class was copied from the HBase source code and modified for use
 * with Lily by NGDATA nv, 2013.
 *
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.lilyproject.repository.impl.hbase;

import com.google.common.base.Preconditions;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.lilyproject.repository.impl.FieldFlags;
import org.lilyproject.repository.impl.hbase.HBaseProtos.CompareType;


import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;



/**
 * This filter is used to filter cells based on value. It takes a {@link CompareFilter.CompareOp}
 * operator (equal, greater, not equal, etc), and either a byte [] value or
 * a WritableByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator
 * is LESS, then we will filter out this cell (return true).  If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier.  Only the value of this column
 * will be tested. When using this filter on a {@link Scan} with specified
 * inputs, the column to be tested should also be added as input (otherwise
 * the filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found
 * on a row, use {@link #setFilterIfMissing}.
 * Otherwise, if the column is found, the entire row will be emitted only if
 * the value passes.  If the value fails, the row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set
 * {@link #setLatestVersionOnly} to false. The default is true, meaning that
 * only the latest version's value is tested and all previous versions are ignored.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
public class LilyFieldSingleColumnValueFilter extends FilterBase {
  static final Log LOG = LogFactory.getLog(LilyFieldSingleColumnValueFilter.class);

  protected byte [] columnFamily;
  protected byte [] columnQualifier;
  private CompareFilter.CompareOp compareOp;
  private ByteArrayComparable comparator;
  private boolean foundColumn = false;
  private boolean matchedColumn = false;
  private boolean filterIfMissing = false;
  private boolean latestVersionOnly = true;

  /**
   * Writable constructor, do not use.
   */
  public LilyFieldSingleColumnValueFilter() {
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public LilyFieldSingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp, final byte[] value) {
    this(family, qualifier, compareOp, new BinaryComparator(value));
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   */

  public LilyFieldSingleColumnValueFilter(byte[] family, byte[] qualifier,
                                          CompareFilter.CompareOp compareOp, ByteArrayComparable comparator) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
    this.comparator = comparator;
  }

    protected LilyFieldSingleColumnValueFilter(byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
                                      ByteArrayComparable comparator, boolean foundColumn, boolean matchedColumn,
                                      boolean filterIfMissing, boolean latestVersionOnly)
    {
        this(family,qualifier,compareOp,comparator);
        this.foundColumn = foundColumn;
        this.matchedColumn = matchedColumn;
        this.filterIfMissing = filterIfMissing;
        this.latestVersionOnly = latestVersionOnly;

    }

  /**
   * @return operator
   */
  public CompareFilter.CompareOp getOperator() {
    return compareOp;
  }

  /**
   * @return the comparator
   */
  public ByteArrayComparable getComparator() {
    return comparator;
  }

  /**
   * @return the family
   */
  public byte[] getFamily() {
    return columnFamily;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return columnQualifier;
  }


  public ReturnCode filterKeyValue(Cell c) {
    KeyValue keyValue = KeyValueUtil.ensureKeyValue(c);
    //System.out.println("REMOVE KEY=" + keyValue.toString() + ", value=" + Bytes.toString(keyValue.getValue()));
    if (this.matchedColumn) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.latestVersionOnly && this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    if (filterColumnValue(keyValue.getBuffer(),
        keyValue.getValueOffset(), keyValue.getValueLength())) {
      return this.latestVersionOnly? ReturnCode.NEXT_ROW: ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final byte [] data, final int offset,
      final int length) {

      // Begin Lily change

      if (!FieldFlags.exists(data[offset])) {
          // a field with deleted marker is the same as a missing field
          return filterIfMissing;
      }

      // Find out if there's metadata appended to the field and if so adjust length so that the metadata is not
      // part of the comparison.
      int metadataEncodingVersion = FieldFlags.getFieldMetadataVersion(data[offset]);
      int lilyFieldValueOffset;
      int lilyFieldValueLength;

      if (metadataEncodingVersion == 0) {
          // there is no metadata
          lilyFieldValueOffset = offset + 1;
          lilyFieldValueLength = length - 1;
      } else if (metadataEncodingVersion == 1) {
          int metadataSize = Bytes.toInt(data, offset + length - Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
          lilyFieldValueOffset = offset + 1; // +1 to skip the field flags
          lilyFieldValueLength = length - 1 - metadataSize - Bytes.SIZEOF_INT;
      } else {
          throw new RuntimeException("Unsupported field metadata encoding version: " + metadataEncodingVersion);
      }


      int compareResult = this.comparator.compareTo(data, lilyFieldValueOffset, lilyFieldValueLength);
      // End Lily change

      switch (this.compareOp) {
          case LESS:
              return compareResult <= 0;
          case LESS_OR_EQUAL:
              return compareResult < 0;
          case EQUAL:
              return compareResult != 0;
          case NOT_EQUAL:
              return compareResult == 0;
          case GREATER_OR_EQUAL:
              return compareResult > 0;
          case GREATER:
              return compareResult >= 0;
          default:
              throw new RuntimeException("Unknown Compare op " + compareOp.name());
      }
  }

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
  }

  public void reset() {
    foundColumn = false;
    matchedColumn = false;
  }

  /**
   * Get whether entire row should be filtered if column is not found.
   * @return true if row should be skipped if column not found, false if row
   * should be let through anyways
   */
  public boolean getFilterIfMissing() {
    return filterIfMissing;
  }

  /**
   * Set whether entire row should be filtered if column is not found.
   * <p>
   * If true, the entire row will be skipped if the column is not found.
   * <p>
   * If false, the row will pass if the column is not found.  This is default.
   * @param filterIfMissing flag
   */
  public void setFilterIfMissing(boolean filterIfMissing) {
    this.filterIfMissing = filterIfMissing;
  }

  /**
   * Get whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @return return value
   */
  public boolean getLatestVersionOnly() {
    return latestVersionOnly;
  }

  /**
   * Set whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @param latestVersionOnly flag
   */
  public void setLatestVersionOnly(boolean latestVersionOnly) {
    this.latestVersionOnly = latestVersionOnly;
  }

  public void setFoundColumn(boolean foundColumn) {
        this.latestVersionOnly = foundColumn;
    }

  public void setMatchedColumn(boolean matchedColumn) {
        this.latestVersionOnly = matchedColumn;
    }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4 ||
                    filterArguments.size() == 6 || filterArguments.size() == 8,
            "Expected 4 or 6 or 8 but got: %s", filterArguments.size());
    byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
    CompareFilter.CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
    ByteArrayComparable comparator = ParseFilter.createComparator(
      ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (compareOp != CompareFilter.CompareOp.EQUAL &&
          compareOp != CompareFilter.CompareOp.NOT_EQUAL) {
        throw new IllegalArgumentException ("A regexstring comparator and substring comparator " +
                                            "can only be used with EQUAL and NOT_EQUAL");
      }
    }

      LilyFieldSingleColumnValueFilter filter = new LilyFieldSingleColumnValueFilter(family, qualifier,
                                                                 compareOp, comparator);

    if (filterArguments.size() >= 6) {
      boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(4));
      boolean latestVersionOnly = ParseFilter.convertByteArrayToBoolean(filterArguments.get(5));
      filter.setFilterIfMissing(filterIfMissing);
      filter.setLatestVersionOnly(latestVersionOnly);
      if (filterArguments.size() == 8) {
        boolean foundColumn = ParseFilter.convertByteArrayToBoolean(filterArguments.get(6));
        boolean matchedColumn = ParseFilter.convertByteArrayToBoolean(filterArguments.get(7));
        filter.setFoundColumn(foundColumn);
        filter.setMatchedColumn(matchedColumn);
      }
    }

    return filter;
  }

  /*public void readFields(final DataInput in) throws IOException {
    this.columnFamily = Bytes.readByteArray(in);
    if(this.columnFamily.length == 0) {
      this.columnFamily = null;
    }
    this.columnQualifier = Bytes.readByteArray(in);
    if(this.columnQualifier.length == 0) {
      this.columnQualifier = null;
    }
    this.compareOp = CompareFilter.CompareOp.valueOf(in.readUTF());
    this.comparator =
      (ByteArrayComparable) HbaseObjectWritable.readObject(in, null);
    this.foundColumn = in.readBoolean();
    this.matchedColumn = in.readBoolean();
    this.filterIfMissing = in.readBoolean();
    this.latestVersionOnly = in.readBoolean();
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeUTF(compareOp.name());
    HbaseObjectWritable.writeObject(out, comparator,
        ByteArrayComparable.class, null);
    out.writeBoolean(foundColumn);
    out.writeBoolean(matchedColumn);
    out.writeBoolean(filterIfMissing);
    out.writeBoolean(latestVersionOnly);
  }*/

    private static ComparatorProtos.Comparator toComparator(ByteArrayComparable comparator) {
        ComparatorProtos.Comparator.Builder builder = ComparatorProtos.Comparator.newBuilder();
        builder.setName(comparator.getClass().getName());
        builder.setSerializedComparator(HBaseZeroCopyByteString.wrap(comparator.toByteArray()));
        return builder.build();
    }

    private LilyFieldSingleColumnValueFilterProto.LilyFieldSingleColumnValueFilter convert() {
        LilyFieldSingleColumnValueFilterProto.LilyFieldSingleColumnValueFilter.Builder builder =
                LilyFieldSingleColumnValueFilterProto.LilyFieldSingleColumnValueFilter.newBuilder();
        if (this.columnFamily != null) {
            builder.setColumnFamily(HBaseZeroCopyByteString.wrap(this.columnFamily));
        }
        if (this.columnQualifier != null) {
            builder.setColumnQualifier(HBaseZeroCopyByteString.wrap(this.columnQualifier));
        }
        HBaseProtos.CompareType compareOp = CompareType.valueOf(this.compareOp.name());
        builder.setCompareOp(compareOp);
        builder.setComparator(toComparator(this.comparator));
        //builder.setFoundColumn(this.foundColumn);
        //builder.setMatchedColumn(this.matchedColumn);
        builder.setFilterIfMissing(this.filterIfMissing);
        builder.setLatestVersionOnly(this.latestVersionOnly);

        return builder.build();
    }

    public byte[] toByteArray() { return convert().toByteArray(); }

    private final static ClassLoader CLASS_LOADER;

    private final static Map<String, Class<?>>
            PRIMITIVES = new HashMap<String, Class<?>>();

    static {
        ClassLoader parent = LilyFieldSingleColumnValueFilter.class.getClassLoader();
        Configuration conf = HBaseConfiguration.create();
        CLASS_LOADER = new DynamicClassLoader(conf, parent);

        PRIMITIVES.put(Boolean.TYPE.getName(), Boolean.TYPE);
        PRIMITIVES.put(Byte.TYPE.getName(), Byte.TYPE);
        PRIMITIVES.put(Character.TYPE.getName(), Character.TYPE);
        PRIMITIVES.put(Short.TYPE.getName(), Short.TYPE);
        PRIMITIVES.put(Integer.TYPE.getName(), Integer.TYPE);
        PRIMITIVES.put(Long.TYPE.getName(), Long.TYPE);
        PRIMITIVES.put(Float.TYPE.getName(), Float.TYPE);
        PRIMITIVES.put(Double.TYPE.getName(), Double.TYPE);
        PRIMITIVES.put(Void.TYPE.getName(), Void.TYPE);
    }

    private static ByteArrayComparable toComparator(ComparatorProtos.Comparator proto)
            throws IOException {
        String type = proto.getName();
        String funcName = "parseFrom";
        byte [] value = proto.getSerializedComparator().toByteArray();
        try {
            Class<? extends ByteArrayComparable> c =
                    (Class<? extends ByteArrayComparable>)Class.forName(type, true, CLASS_LOADER);
            Method parseFrom = c.getMethod(funcName, byte[].class);
            if (parseFrom == null) {
                throw new IOException("Unable to locate function: " + funcName + " in type: " + type);
            }
            return (ByteArrayComparable)parseFrom.invoke(null, value);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static LilyFieldSingleColumnValueFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        LilyFieldSingleColumnValueFilterProto.LilyFieldSingleColumnValueFilter proto;
        try {
            proto = LilyFieldSingleColumnValueFilterProto.LilyFieldSingleColumnValueFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        final CompareOp compareOp =
                CompareOp.valueOf(proto.getCompareOp().name());
        final ByteArrayComparable comparator;
        try {
            comparator = toComparator(proto.getComparator());
        } catch (IOException ioe) {
            throw new DeserializationException(ioe);
        }

        return new LilyFieldSingleColumnValueFilter(proto.hasColumnFamily() ? proto.getColumnFamily().toByteArray() : null,
                proto.hasColumnQualifier() ? proto.getColumnQualifier().toByteArray() : null,
                compareOp, comparator, proto.getFoundColumn(), proto.getMatchedColumn(),
                proto.getFilterIfMissing(), proto.getLatestVersionOnly());
    }

  @Override
  public String toString() {
    return String.format("%s (%s, %s, %s, %s)",
        this.getClass().getSimpleName(), Bytes.toStringBinary(this.columnFamily),
        Bytes.toStringBinary(this.columnQualifier), this.compareOp.name(),
        Bytes.toStringBinary(this.comparator.getValue()));
  }
}
