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
package org.lilyproject.hbaseext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

public class ContainsValueComparator extends WritableByteArrayComparable {
    private byte[] nestingLevelAndValue;
    private int offset;

    /**
     * Nullary constructor, for Writable
     */
    public ContainsValueComparator() {
        super();
    }

    /**
     * Constructor.
     */
    public ContainsValueComparator(byte[] nestingLevelAndValue) {
        this.nestingLevelAndValue = nestingLevelAndValue;
    }

    @Override
    public byte[] getValue() {
        return nestingLevelAndValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nestingLevelAndValue = Bytes.readByteArray(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, nestingLevelAndValue);
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
        byte[] ourStoreKey = Bytes.tail(nestingLevelAndValue, nestingLevelAndValue.length - Bytes.SIZEOF_INT);
        if (theirValue == null && ourStoreKey == null) {
            return 0;
        }
        if (theirValue.length == 0 && ourStoreKey.length == 0) {
            return 0;
        }
        if (theirValue.length < ourStoreKey.length) {
            return -1;
        }
        if (theirValue[0] == (byte)(1)) { // First byte indicates if it was deleted or not
            return -1;
        }

        int nestingLevel = Bytes.toInt(nestingLevelAndValue);
        offset = 1;

        return compareBlob(nestingLevel, ourStoreKey, theirValue);
    }

    private int compareBlob(int nestingLevel, byte[] ourStoreKey, byte[] theirValue) {
        int compareTo = -1;
        if (0 == nestingLevel) {
            compareTo = compareBlob(ourStoreKey, theirValue);
            if (0 == compareTo) {
                return 0;
            }
            skipRestOfBlob(theirValue);
        } else {
            int count = readInt(theirValue); // Number of elements in the list or path
            for (int i = 0; i < count; i++) {
                compareTo = compareBlob(nestingLevel - 1, ourStoreKey, theirValue);
                if (0 == compareTo) {
                    return 0;
                }
            }
        }
        return compareTo;
    }

    /**
     * Compares the value of the blob with ourStoreKey
     */
    private int compareBlob(byte[] ourStoreKey, byte[] theirValue) {
        offset++; // Skip the encoding byte. Currently there is only one encoding version so we can ignore it.
        int blobValueLength = readVInt(theirValue); // Length of the blob value
        int compareTo = Bytes.compareTo(ourStoreKey, 0, ourStoreKey.length, theirValue, offset, blobValueLength);
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
