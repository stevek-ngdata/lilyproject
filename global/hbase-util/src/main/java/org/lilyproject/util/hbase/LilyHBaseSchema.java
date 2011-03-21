package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class LilyHBaseSchema {
    public static final byte EXISTS_FLAG = (byte) 0;
    public static final byte DELETE_FLAG = (byte) 1;
    public static final byte[] DELETE_MARKER = new byte[] { DELETE_FLAG };
    

    private static final HTableDescriptor recordTableDescriptor;

    static {
        recordTableDescriptor = new HTableDescriptor(Table.RECORD.bytes);
        recordTableDescriptor.addFamily(new HColumnDescriptor(RecordCf.DATA.bytes,
                HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
        recordTableDescriptor.addFamily(new HColumnDescriptor(RecordCf.ROWLOG.bytes));
    }

    private static final HTableDescriptor typeTableDescriptor;

    static {
        typeTableDescriptor = new HTableDescriptor(Table.TYPE.bytes);
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.DATA.bytes));
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.FIELDTYPE_ENTRY.bytes, HConstants.ALL_VERSIONS,
                "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
        typeTableDescriptor.addFamily(new HColumnDescriptor(TypeCf.MIXIN.bytes, HConstants.ALL_VERSIONS, "none",
                false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
    }
    
    private static final HTableDescriptor blobIncubatorDescriptor;
    
    static {
        blobIncubatorDescriptor = new HTableDescriptor(Table.BLOBINCUBATOR.bytes);
        blobIncubatorDescriptor.addFamily(new HColumnDescriptor(BlobIncubatorCf.REF.bytes));
    }

    public static HTableInterface getRecordTable(HBaseTableFactory tableFactory) throws IOException {
        return tableFactory.getTable(recordTableDescriptor);
    }

    public static HTableInterface getTypeTable(HBaseTableFactory tableFactory) throws IOException {
        return tableFactory.getTable(typeTableDescriptor);
    }
    
    public static HTableInterface getBlobIncubatorTable(HBaseTableFactory tableFactory, boolean clientMode) throws IOException {
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
        DATA("data"), // The actual data fields and system fields of records are stored in the same column family
        ROWLOG("rowlog"); // Column family for the rowlog payload and executionstate

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
        VERSION("version"),
        LOCK("lock"),
        DELETED("deleted"),
        NON_VERSIONED_RT_ID("nv-rt"),
        NON_VERSIONED_RT_VERSION("nv-rtv"),
        VERSIONED_RT_ID("v-rt"),
        VERSIONED_RT_VERSION("v-rtv"),
        VERSIONED_MUTABLE_RT_ID("vm-rt"),
        VERSIONED_MUTABLE_RT_VERSION("vm-rtv");

        public final byte[] bytes;
        public final String name;
        // The fields and system fields of records are stored in the same column family : DATA
        public static final byte SYSTEM_PREFIX = (byte)1; // Prefix for the column-qualifiers of system fields 
        public static final byte DATA_PREFIX = (byte)2; // Prefix for the column-qualifiers of actual data fields
        
        // The payload and executionstat of the rowlogs are stored in the same column family : ROWLOG
        public static final byte WAL_PREFIX = (byte)3; // Prefix for the column-qualifiers of the WAL rowlog  
        public static final byte MQ_PREFIX = (byte)4; // Prefix for the column-qualifiers of the MQ rowlog
        
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
        MIXIN("mixin");

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
