package org.lilyproject.repository.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

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
        byte[] ourStoreKey = Bytes.tail(valueTypeAndValue, valueTypeAndValue.length-Bytes.SIZEOF_INT);
        if (theirValue == null && ourStoreKey == null)
            return 0;
        if (theirValue.length == 0 && ourStoreKey.length == 0)
            return 0;
        if (theirValue.length < ourStoreKey.length)
            return -1;
        if (theirValue[0] == (byte)(1)) {
            return -1;
        }
        // Multivalue and hierarchical
        int offset = 1; // First byte indicates if it was deleted or not
        if (2 == valueTypeCode) {
            int compareTo = -1;
            while (offset < theirValue.length) {
                int multivalueKeyLength = Bytes.toInt(theirValue, offset); // Length of the next multivalue key
                offset = offset + Bytes.SIZEOF_INT;
                int stopIndex = offset + multivalueKeyLength;
                while (offset < stopIndex) {
                    int hierarchycalKeyLength = Bytes.toInt(theirValue, offset); // Length of the next hierarchy key
                    offset = offset + Bytes.SIZEOF_INT;
                    int valueLength = Bytes.toInt(theirValue, offset); // Length of the blob key
                    // Don't increase offset here, it's calculated in the hierarchycalKeyLength
                    compareTo = Bytes.compareTo(ourStoreKey, 0, ourStoreKey.length, theirValue, offset + Bytes.SIZEOF_INT, valueLength);
                    if (0 == compareTo)
                        return 0;
                    offset = offset + hierarchycalKeyLength;
                }
            }
            return compareTo;
        }
        // Mutlivalue or hierarchical
        if (1 == valueTypeCode || 1 == valueTypeCode) {
            int compareTo = -1;
            while (offset < theirValue.length) {
                int multiValueKeyLength = Bytes.toInt(theirValue, offset); // Length of the next multivalue or hierarchy key
                offset = offset + Bytes.SIZEOF_INT;
                int valueLength = Bytes.toInt(theirValue, offset); // Length of the blob key
             // Don't increase offset here, it's calculated in the multivalueKeyLength
                compareTo = Bytes.compareTo(ourStoreKey, 0, ourStoreKey.length, theirValue, offset + Bytes.SIZEOF_INT, valueLength);
                if (0 == compareTo)
                    return 0;
                offset = offset + multiValueKeyLength;
            }
            return -1;
        }
        int valueLength = Bytes.toInt(theirValue, offset); // Length of the blob key, everything beyond that is mediatype, size, filename etc.
        offset = offset + Bytes.SIZEOF_INT; 
        return Bytes.compareTo(ourStoreKey, 0, ourStoreKey.length, theirValue, offset, valueLength);
    }
}
