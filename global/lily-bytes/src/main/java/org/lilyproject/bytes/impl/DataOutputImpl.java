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
package org.lilyproject.bytes.impl;

import java.util.Arrays;

import org.lilyproject.bytes.api.DataOutput;

/**
 * Implementation of {@link DataOutput} which writes and encodes primitve values to a byte[].
 * This byte[] can then be used in the constructor of {@link DataInputImpl}.
 *
 * <p>The position within the underlying byte[] is maintained so that each write
 *    call will append the next encoded value in the byte[].
 *
 * <p>The underlying byte[] is resized when it is not large enough to contain the next value to be written.
 *
 * <p>This implementation (especially #writeUTF()) is based on (and some pieces are copied from) the work
 *    done by Lucene in the methods <code>UTF16toUTF8</code> and <code>UTF8toUTF16</code>
 *    in <code>org.apache.lucene.util.UnicodeUtil.java</code> (revision 1030754),
 *    and combined with the work done by ElasticSearch in
 *    <code>org.elasticsearch.common.io.stream.BytesStreamInput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.BytesStreamOutput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.StreamInput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.StreamOutput.java</code>.
 *
 */

public class DataOutputImpl implements DataOutput {
    public static final int UNI_SUR_HIGH_START = 0xD800;
    public static final int UNI_SUR_LOW_START = 0xDC00;

    private static final long HALF_SHIFT = 10;

    private static final int SURROGATE_OFFSET =
        Character.MIN_SUPPLEMENTARY_CODE_POINT - (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

    private byte[] buffer;
    /** The position at which the next item will be added. */
    private int pos = 0;

    /**
     * Default constructor.
     * When it is possible to give a good estimate of the number of bytes
     * that will be written, it is better to use {@link DataOutputImpl(int)}.
     */
    public DataOutputImpl() {
        this(256);
    }

    /**
     * Constructor for <code>DataOutputImpl</code>
     * @param sizeEstimate estimated size for the underlying byte[],
     *        a good estimate can avoid that the byte[] needs to be resized, or that too many bytes are allocated.
     */
    public DataOutputImpl(int sizeEstimate) {
        buffer = new byte[sizeEstimate];
    }

    @Override
    public byte[] toByteArray() {
        return Arrays.copyOfRange(buffer, 0, pos);
    }

    /**
     * Checks if the buffer has enough space to put <code>len</code> bytes.
     * If not the buffer is resized to at least twice its current size.
     */
    private void assureSize(int len) {
        int newcount = pos + len;
        if (newcount > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, newcount));
        }
    }

    @Override
    public void writeByte(byte b) {
        assureSize(1);
        buffer[pos++] = b;
    }

    /**
     * Writes a byte to the byte[] without checking that there is enough space in the byte[].
     * @param b
     */
    private void writeByteUnsafe(byte b) {
        buffer[pos++] = b;
    }

    @Override
    public void writeBytes(byte[] bytes) {
        int length = bytes.length;
        assureSize(length);
        System.arraycopy(bytes, 0, buffer, pos, length);
        pos += length;
    }

    /**
     * Encodes a string to (unmodified) UTF-8 bytes and puts it in the buffer.
     */
    @Override
    public void writeUTF(String string) {
        writeUTF(string, true);
    }

    @Override
    public void writeVUTF(String string) {
        writeUTF(string, true, true);
    }

    @Override
    public void writeUTF(String string, boolean includeLength) {
        writeUTF(string, includeLength, false);
    }

    private void writeUTF(String string, boolean includeLength, boolean useVInt) {
        if (string == null) {
            writeInt(-1);
            return;
        }

        int strlen = string.length();
        int utflen = 0;

        // First calculate the utflen
        int i = 0;
        while(i < strlen) {
            final int code = string.charAt(i++);
            if (code < 0x80) {
                utflen++;
            } else if (code < 0x800) {
                utflen += 2;
            } else if (code < 0xD800 || code > 0xDFFF) {
                utflen += 3;
            } else {
                // surrogate pair
                // confirm valid high surrogate
                if (code < 0xDC00 && i < strlen) {
                    int utf32 = string.charAt(i);
                    // confirm valid low surrogate and write pair
                    if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
                        utflen += 4;
                        i++;
                        continue;
                    }
                }
                utflen += 3;
            }
        }


        assureSize(4 + utflen); // Make sure the buffer has enough space to put the bytes for the length and the string

        if (includeLength) {
            // Write the length in the buffer
            if (useVInt) {
                writeVIntUnsafe(utflen);
            } else {
                writeIntUnsafe(utflen);
            }
        }

        int ch = 0; // Character from the string

        // Optimized for loop as long as the characters can be encoded as one byte
        for (i = 0; i < strlen; i++) {
            ch = string.charAt(i);
            if (!(ch < 0x80)) {
                break; // Once we encounter a character that should be encoded with >1 byte we jump out of this optimized loop
            }
            buffer[pos++] = (byte) ch;
        }

        while(i < strlen) {
            ch = (int) string.charAt(i++);

            if (ch< 0x80) {
                buffer[pos++] = (byte)ch;
            } else if (ch < 0x800) {
                buffer[pos++] = (byte) (0xC0 | (ch >> 6));
                buffer[pos++] = (byte)(0x80 | (ch & 0x3F));
            } else if (ch < 0xD800 || ch > 0xDFFF) {
                buffer[pos++] = (byte)(0xE0 | (ch >> 12));
                buffer[pos++] = (byte)(0x80 | ((ch >> 6) & 0x3F));
                buffer[pos++] = (byte)(0x80 | (ch & 0x3F));
            } else {
                // surrogate pair
                // confirm valid high surrogate
                if (ch < 0xDC00 && i < strlen) {
                    int utf32 = string.charAt(i);
                    // confirm valid low surrogate and write pair
                    if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
                        utf32 = (ch << 10) + utf32 + SURROGATE_OFFSET;
                        i++;
                        buffer[pos++] = (byte)(0xF0 | (utf32 >> 18));
                        buffer[pos++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
                        buffer[pos++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
                        buffer[pos++] = (byte)(0x80 | (utf32 & 0x3F));
                        continue;
                    }
                }
                // replace unpaired surrogate or out-of-order low surrogate
                // with substitution character
                buffer[pos++] = (byte) 0xEF;
                buffer[pos++] = (byte) 0xBF;
                buffer[pos++] = (byte) 0xBD;
            }
        }
    }

    @Override
    public void writeInt(int integer) {
        assureSize(4); // Make sure the buffer has enough space
        writeIntUnsafe(integer);
    }

    /**
     *  Writes the int without checking if there is enough space for it
     */
    private void writeIntUnsafe(int integer) {
        buffer[pos++] = (byte) (integer >> 24);
        buffer[pos++] = (byte) (integer >> 16);
        buffer[pos++] = (byte) (integer >> 8);
        buffer[pos++] = (byte) (integer);
    }


    private static byte ZERO = 0;
    private static byte ONE = 1;

    @Override
    public void writeBoolean(boolean b) {
        assureSize(1);
        writeByteUnsafe(b ? ONE : ZERO);
    }

    @Override
    public void writeDouble(double value) {
        writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void writeLong(long value) {
        assureSize(8);
        writeIntUnsafe((int) (value >> 32));
        writeIntUnsafe((int) value);
    }

    @Override
    public void writeShort(int value) {
        assureSize(2);
        writeByteUnsafe((byte) (value >> 8));
        writeByteUnsafe((byte) value);
    }

    @Override
    public void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    /**
    * Writes an int in a variable-length format. Writes between one and
    * five bytes. Smaller values take fewer bytes. Negative numbers are not
    * supported.
    */
    @Override
    public void writeVInt(int i) {
        assureSize(5);
        writeVIntUnsafe(i);
    }

    /**
     * Same as writeVInt(), but without checking that there is enough space for it.
     */
    private void writeVIntUnsafe(int i) {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByteUnsafe((byte) i);
    }

    /**
    * Writes a long in a variable-length format. Writes between one and five
    * bytes. Smaller values take fewer bytes. Negative numbers are not
    * supported.
    */
    @Override
    public void writeVLong(long i) {
        assureSize(5);
        writeVLongUnsafe(i);
    }

    /**
     * Same as writeVLong(), but without checking that there is enough space for it.
     */
    private void writeVLongUnsafe(long i) {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByteUnsafe((byte) i);
    }

    @Override
    public int getSize() {
        return pos;
    }
}
