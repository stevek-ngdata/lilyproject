package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

public class IndexFieldDefinition {
    private String name;
    private IndexValueType type;

    public IndexFieldDefinition(String name, IndexValueType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public IndexValueType getType() {
        return type;
    }

    /**
     * The number of bytes this entry takes in the index row key.
     */
    public int getByteLength() {
        return Bytes.SIZEOF_INT;

    }

    public int toBytes(byte[] bytes, int offset, Object value) {
        return Bytes.putInt(bytes, offset, ((Integer)value).intValue());
    }
}
