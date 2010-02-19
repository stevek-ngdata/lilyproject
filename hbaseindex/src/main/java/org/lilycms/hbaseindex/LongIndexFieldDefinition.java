package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.node.ObjectNode;

public class LongIndexFieldDefinition extends IndexFieldDefinition {
    public LongIndexFieldDefinition(String name) {
        super(name, IndexValueType.LONG);
    }

    public LongIndexFieldDefinition(String name, ObjectNode jsonObject) {
        this(name);
    }

    @Override
    public int getLength() {
        return Bytes.SIZEOF_LONG;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        long longValue = (Long)value;
        int nextOffset = Bytes.putLong(bytes, offset, longValue);

        // To make the longs sort correctly when comparing their binary
        // representations, we need to invert the sign bit
        bytes[offset] = (byte)(bytes[offset] ^ 0x80);

        return nextOffset;
    }
}
