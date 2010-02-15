package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

public class IntegerIndexFieldDefinition extends IndexFieldDefinition {
    public IntegerIndexFieldDefinition(String name) {
        super(name, IndexValueType.INTEGER);
    }

    @Override
    public int getByteLength() {
        return Bytes.SIZEOF_INT;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        int integer = (Integer)value;
        int nextOffset = Bytes.putInt(bytes, offset, integer);

        // To make the integers sort correctly when comparing their binary
        // representations, we need to flip the sign bit
        if (integer < 0) {
            bytes[offset] = (byte)(bytes[offset] & 0x7F);
        } else { // if (integer >= 0)
            bytes[offset] = (byte)(bytes[offset] | 0x80);
        }

        return nextOffset;
    }
}
