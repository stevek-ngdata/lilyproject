package org.lilyproject.repository.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.ValueType;

public class ContainsValueComparator extends WritableByteArrayComparable {
    private byte[] valueTypeAndValue;

    /**
     * Nullary constructor, for Writable
     */
    public ContainsValueComparator() {
        super();
    }
    
    /**
     * Constructor.
     * 
     * @param value2
     *            the value to compare against
     */
    public ContainsValueComparator(byte[] valueTypeAndValue) {
        this.valueTypeAndValue = valueTypeAndValue;
    }

    public byte[] getValue() {
        return valueTypeAndValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valueTypeAndValue = Bytes.readByteArray(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, valueTypeAndValue);
    }

    @Override
    public int compareTo(byte[] theirValue) {
        int valueTypeCode = Bytes.toInt(valueTypeAndValue);
        byte[] ourValue = Bytes.tail(valueTypeAndValue, valueTypeAndValue.length-Bytes.SIZEOF_INT);
        if (theirValue == null && ourValue == null)
            return 0;
        if (theirValue.length == 0 && ourValue.length == 0)
            return 0;
        if (theirValue.length < ourValue.length)
            return -1;
        if (theirValue[0] == (byte)(1)) {
            return -1;
        }
        // Multivalue and hierarchical
        int offset = 1;
        if (2 == valueTypeCode) {
            int compareTo = -1;
            while (offset < theirValue.length) {
                int multivalueLength = Bytes.toInt(theirValue, offset);
                offset = offset + Bytes.SIZEOF_INT;
                int nextMultivalueOffset = offset+multivalueLength;
                while (offset < nextMultivalueOffset) {
                    int valueLength = Bytes.toInt(theirValue, offset);
                    offset = offset + Bytes.SIZEOF_INT;
                    compareTo = Bytes.compareTo(ourValue, 0, ourValue.length, theirValue, offset, valueLength);
                    if (0 == compareTo)
                        return 0;
                    offset = offset + valueLength;
                }
            }
            return compareTo;
        }
        // Mutlivalue or hierarchical
        if (1 == valueTypeCode || 1 == valueTypeCode) {
            int compareTo = -1;
            while (offset < theirValue.length) {
                int valueLength = Bytes.toInt(theirValue, offset);
                offset = offset + Bytes.SIZEOF_INT;
                compareTo = Bytes.compareTo(ourValue, 0, ourValue.length, theirValue, offset, valueLength);
                if (0 == compareTo)
                    return 0;
                offset = offset + valueLength;
            }
            return -1;
        }
        int valueLength = Bytes.toInt(theirValue, offset);
        return Bytes.compareTo(ourValue, 0, ourValue.length, theirValue, offset, valueLength);
    }
}
