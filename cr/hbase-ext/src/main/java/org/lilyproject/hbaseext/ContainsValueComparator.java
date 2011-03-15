package org.lilyproject.hbaseext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainsValueComparator extends WritableByteArrayComparable {
    private byte[] valueTypeAndValue;
    private int offset;

    /**
     * Nullary constructor, for Writable
     */
    public ContainsValueComparator() {
        super();
    }
    
    /**
     * Constructor.
     * 
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
    /**
     * Checks if a blob key (ourStoreKey) is contained in the blob field (theirValue).
     * The blob field can be a multivalue and / or hierarchical field.
     * 
     * <p>IMPORTANT: This implementation depends on the byte encodings from ValueTypeImpl, BlobValueType and DataOutputImpl.
     *               Any changes there have an impact on this implementation. 
     */
    public int compareTo(byte[] theirValue) {
        int valueTypeCode = Bytes.toInt(valueTypeAndValue);
        byte[] ourStoreKey = Bytes.tail(valueTypeAndValue, valueTypeAndValue.length-Bytes.SIZEOF_INT);
        if (theirValue == null && ourStoreKey == null)
            return 0;
        if (theirValue.length == 0 && ourStoreKey.length == 0)
            return 0;
        if (theirValue.length < ourStoreKey.length)
            return -1;
        if (theirValue[0] == (byte)(1)) { // First byte indicates if it was deleted or not
            return -1;
        }
        offset = 1;
        if (2 == valueTypeCode) {
            int compareTo = -1;
            int multivalueCount = readInt(theirValue); // Number of elements in the multivalue
            for (int i = 0; i < multivalueCount; i++) {
                int hierarchicalCount = readInt(theirValue); // Number of elements in the hierarchy
                for (int j = 0; j < hierarchicalCount; j++) {
                    compareTo = compareBlob(ourStoreKey, theirValue);
                    if (0 == compareTo)
                        return 0;
                    skipRestOfBlob(theirValue);
                }
            }
            return compareTo;
        }
        // Mutlivalue or hierarchical
        if (1 == valueTypeCode || 1 == valueTypeCode) {
            int compareTo = -1;
            int count = readInt(theirValue); // Number of elements in the multivalue or hierarchy
            for (int i = 0; i < count; i++) {
                compareTo = compareBlob(ourStoreKey, theirValue);
                if (0 == compareTo)
                    return 0;
                skipRestOfBlob(theirValue);
            }
            return compareTo;
        }
        return compareBlob(ourStoreKey, theirValue);
    }
    
    /**
     * Compares the value of the blob with ourStoreKey
     */
    private int compareBlob(byte[] ourStoreKey, byte[] theirValue) {
        int blobValueLength = readVInt(theirValue); // Length of the blob value
        int compareTo = Bytes.compareTo(ourStoreKey, 0, ourStoreKey.length, theirValue, offset , blobValueLength);
        offset += blobValueLength;
        return compareTo;
    }
    
    /**
     * Skips the rest of the blob (media type, blob size, name) and puts the offset to the next value to be read
     */
    private void skipRestOfBlob(byte[] theirValue) {
        int mediaTypeLength = readInt(theirValue); // Length of the blob media type (offset = offset + blobvaluelength_size + blobvalue_size)
        offset += mediaTypeLength;
        offset += 8; // Blob size (long)
        int nameLength = readInt(theirValue); // Length of the blob name (offset = offset + blobvaluelength_size + blobvalue_size + mediaTypeLength_size + mediaType_size + blobsize_long)
        offset += nameLength;
    }
    
    private int readInt(byte[] bytes) {
        return ((bytes[offset++] & 0xFF) << 24) | ((bytes[offset++] & 0xFF) << 16)
        | ((bytes[offset++] & 0xFF) << 8) | (bytes[offset++] & 0xFF);
    }
    
    /**
     * Reads an int stored in variable-length format. Reads between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     */
     public int readVInt(byte[] bytes) {
         byte b = bytes[offset++];
         int i = b & 0x7F;
         for (int shift = 7; (b & 0x80) != 0; shift += 7) {
             b = bytes[offset++];
             i |= (b & 0x7F) << shift;
         }
         return i;
     }
}
