package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.node.ObjectNode;

public class IntegerIndexFieldDefinition extends IndexFieldDefinition {
    public IntegerIndexFieldDefinition(String name) {
        super(name, IndexValueType.INTEGER);
    }

    public IntegerIndexFieldDefinition(String name, ObjectNode jsonObject) {
        this(name);
    }

    @Override
    public int getLength() {
        return Bytes.SIZEOF_INT;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        int integer = (Integer)value;
        int nextOffset = Bytes.putInt(bytes, offset, integer);

        // To make the integers sort correctly when comparing their binary
        // representations, we need to invert the sign bit
        bytes[offset] = (byte)(bytes[offset] ^ 0x80);

        return nextOffset;
    }
}
