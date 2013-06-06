/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.util.hbase;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class LilyHBaseSchema {
    private static final HColumnDescriptor DATA_CF;

    public static final byte[] TABLE_TYPE_PROPERTY = Bytes.toBytes("lilyTableType");
    public static final byte[] TABLE_TYPE_RECORD = Bytes.toBytes("record");

    static {
        DATA_CF = new HColumnDescriptor(RecordCf.DATA.bytes,
                HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER);
        DATA_CF.setScope(1);    // replication scope: HBase docs: "a scope of 0 (default) means that it won't be
                                // replicated and a scope of 1 means it's going to be. In the future, different scope can
                                // be used for routing policies."
    }

    private static final HTableDescriptor typeTableDescriptor;

    static {
        typeTableDescriptor = new HTableDescriptor(Table.TYPE.bytes);
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.DATA.bytes));
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.FIELDTYPE_ENTRY.bytes, HConstants.ALL_VERSIONS,
                "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.SUPERTYPE.bytes, HConstants.ALL_VERSIONS, "none",
                false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
    }

    private static final HTableDescriptor blobIncubatorDescriptor;

    static {
        blobIncubatorDescriptor = new HTableDescriptor(Table.BLOBINCUBATOR.bytes);
        blobIncubatorDescriptor.addFamily(new HColumnDescriptor(BlobIncubatorCf.REF.bytes));
    }

    private LilyHBaseSchema() {
    }

    @VisibleForTesting
    static HTableDescriptor createRecordTableDescriptor(String repositoryName, String tableName) {

        // We have checks on table name in TableManagerImpl -- probably this can go
        if (tableName.contains(".") || tableName.contains(":")) {
            throw new IllegalArgumentException("Repository table name cannot contain periods or colons");
        }

        String hbaseTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, tableName);
        HTableDescriptor recordTableDescriptor = new HTableDescriptor(hbaseTableName);
        recordTableDescriptor.addFamily(DATA_CF);
        recordTableDescriptor.setValue(TABLE_TYPE_PROPERTY, TABLE_TYPE_RECORD);
        RepoAndTableUtil.setRepositoryOwnership(recordTableDescriptor, repositoryName);
        return recordTableDescriptor;
    }

    public static boolean isRecordTableDescriptor(HTableDescriptor htableDescriptor) {
        byte[] value = htableDescriptor.getValue(TABLE_TYPE_PROPERTY);
        return value != null && Bytes.equals(value, TABLE_TYPE_RECORD);
    }

    public static HTableInterface getRecordTable(HBaseTableFactory tableFactory, String repositoryName, String tableName) throws IOException, InterruptedException {
        HTableInterface recordTable = tableFactory.getTable(createRecordTableDescriptor(repositoryName, tableName));
        verifyIsRecordTable(recordTable.getTableDescriptor());
        return recordTable;
    }

    public static HTableInterface getRecordTable(HBaseTableFactory tableFactory, String repositoryName, String tableName, byte[][] splitKeys) throws IOException, InterruptedException {
        HTableInterface recordTable = tableFactory.getTable(createRecordTableDescriptor(repositoryName, tableName), splitKeys);
        verifyIsRecordTable(recordTable.getTableDescriptor());
        return recordTable;
    }

    public static HTableInterface getRecordTable(HBaseTableFactory tableFactory, String repositoryName, String tableName, boolean clientMode) throws IOException, InterruptedException {
        HTableInterface recordTable = tableFactory.getTable(createRecordTableDescriptor(repositoryName, tableName), !clientMode);
        verifyIsRecordTable(recordTable.getTableDescriptor());
        return recordTable;
    }

    private static void verifyIsRecordTable(HTableDescriptor htableDescriptor) {
        if (!isRecordTableDescriptor(htableDescriptor)) {
            throw new IllegalArgumentException(htableDescriptor.getNameAsString() + " is not a valid record table");
        }
    }

    public static HTableInterface getTypeTable(HBaseTableFactory tableFactory) throws IOException, InterruptedException {
        return tableFactory.getTable(typeTableDescriptor);
    }

    public static HTableInterface getBlobIncubatorTable(HBaseTableFactory tableFactory, boolean clientMode) throws IOException, InterruptedException {
        return tableFactory.getTable(blobIncubatorDescriptor, !clientMode);
    }

    public static enum Table {
        RECORD("record"),
        TYPE("type"),
        BLOBINCUBATOR("blobincubator");

        public final byte[] bytes;
        public final String name;

        Table(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Column families in the record table.
     */
    public static enum RecordCf {
        DATA("data"); // The actual data fields and system fields of records are stored in the same column family

        public final byte[] bytes;
        public final String name;

        RecordCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Columns in the record table.
     */
    public static enum RecordColumn {
        /** occ = optimistic concurrency control (a version counter) */
        OCC("occ"),
        VERSION("version"),
        DELETED("deleted"),
        NON_VERSIONED_RT_ID("nv-rt"),
        NON_VERSIONED_RT_VERSION("nv-rtv"),
        VERSIONED_RT_ID("v-rt"),
        VERSIONED_RT_VERSION("v-rtv"),
        VERSIONED_MUTABLE_RT_ID("vm-rt"),
        VERSIONED_MUTABLE_RT_VERSION("vm-rtv"),
        /** payload for the event dispatcher */
        PAYLOAD("pl");

        public final byte[] bytes;
        public final String name;
        // The fields and system fields of records are stored in the same column family : DATA
        public static final byte SYSTEM_PREFIX = (byte)1; // Prefix for the column-qualifiers of system fields
        public static final byte DATA_PREFIX = (byte)2; // Prefix for the column-qualifiers of actual data fields

        RecordColumn(String name) {
            this.name = name;
            this.bytes = Bytes.add(new byte[]{SYSTEM_PREFIX},Bytes.toBytes(name));
        }
    }

    /**
     * Column families in the type table.
     */
    public static enum TypeCf {
        DATA("data"),
        FIELDTYPE_ENTRY("fieldtype-entry"),
        SUPERTYPE("mixin") /* actual CF name is 'mixin' for backwards compatibility */;

        public final byte[] bytes;
        public final String name;

        TypeCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Columns in the type table.
     */
    public static enum TypeColumn {
        VERSION("version"),
        RECORDTYPE_NAME("rt"),
        FIELDTYPE_NAME("ft"),
        FIELDTYPE_VALUETYPE("vt"),
        FIELDTYPE_SCOPE("scope"),
        CONCURRENT_COUNTER("cc"),
        CONCURRENT_TIMESTAMP("cts");

        public final byte[] bytes;
        public final String name;

        TypeColumn(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Column families in the blob incubator table.
     */
    public static enum BlobIncubatorCf {
        REF("ref");

        public final byte[] bytes;
        public final String name;

        BlobIncubatorCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Columns in the blob incubator table.
     */
    public static enum BlobIncubatorColumn {
        RECORD("record"), FIELD("field");

        public final byte[] bytes;
        public final String name;

        BlobIncubatorColumn(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }
}
