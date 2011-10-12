package org.lilyproject.bytes.impl;

import org.lilyproject.bytes.api.DataInput;

/**
 * Implementation of {@link DataInput} which reads primitve values from an underlying byte[].
 * The byte[] should have been created, encoded by the related to {@link DataOutputImpl}.
 * 
 * <p>The position within the underlying byte[] is maintained so that each read
 *    call will return the next value in the byte[].
 * 
 * <p>This implementation (especially #readUTF()) is based on (and some pieces are copied from) the work
 *    done by Lucene in the methods <code>UTF16toUTF8</code> and <code>UTF8toUTF16</code> 
 *    in <code>org.apache.lucene.util.UnicodeUtil.java</code> (revision 1030754),
 *    and combined with the work done by ElasticSearch in 
 *    <code>org.elasticsearch.common.io.stream.BytesStreamInput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.BytesStreamOutput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.StreamInput.java</code>,
 *    <code>org.elasticsearch.common.io.stream.StreamOutput.java</code>.
 * 
 * <p>See also <a href=http://en.wikipedia.org/wiki/UTF-16/UCS-2>http://en.wikipedia.org/wiki/UTF-16/UCS-2</a>
 */
public class DataInputImpl implements DataInput {
    public static final int UNI_SUR_LOW_START = 0xDC00;
    
    private static final long UNI_MAX_BMP = 0x0000FFFF;
    
    private static final long HALF_SHIFT = 10;
    private static final long HALF_MASK = 0x3FFL;

    private final byte[] source; // The underlying byte[]
    private int startPosition;
    private int pos; // Position of the next value to be read
    private int size;
    
    // Character array build while reading a string.
    // The same char array is reused for each read, avoiding to allocated a new array each time.
    // It is resized it when needed.
    private char[] chararr = new char[80]; 

    /**
     * Constructor for the {@link DataInput}.
     * The source is the underlying byte[] from which the data will be read.
     * It should have been created using {@link DataOutputImpl}.
     */
    public DataInputImpl(byte[] source) {
        this.source = source;
        this.startPosition = 0;
        this.pos = 0;
        this.size = source.length;
    }

    /**
     * Constructor for the {@link DataInput} based on an existing DataInputImpl.
     * Its source (the underlying byte[]) is the same as for the given dataInput.
     * 
     * @param startPosition position within the source, relative to the startPosition of the given dataInput
     * @param size the size of the DataInput
     * The source is a sub-array of the underlying byte[] from which the data will be read,
     * limited between startPosition en startPosition+length
     * It should have been created using {@link DataOutputImpl}.
     */
    public DataInputImpl(DataInputImpl dataInput, int startPosition, int size) {
        this.source = dataInput.source;
        this.pos = dataInput.startPosition + startPosition;
        this.size = size;
    }

    
    @Override
    public byte readByte() {
        return source[pos++];
    }

    @Override
    public byte[] readBytes(int length) {
        byte[] result = new byte[length];
        System.arraycopy(source, pos, result, 0, length);
        pos += length;
        return result;
    }
    
    /**
     * Reads an (unmodified)UTF-8 from the underlying byte[].
     * @return null when the utflen == -1, meaning a null-string was written by {@link DataOutputImpl#writeUTF(String)}
     * @return empty string when the utflen == 0, meaning an empty string was written by {@link DataOutputImpl#writeUTF(String)}
     * @return the string written {@link DataOutputImpl#writeUTF(String)} in all other cases.
     */
    @Override
    public String readUTF() {
        int utflen = readInt();
        if (utflen == -1)
            return null;
        if (utflen == 0)
            return new String();
        int count = pos;
        int endPos = pos + utflen;
        // Resize the chararr if it is not large enough.
        if (chararr.length < utflen) {
            chararr = new char[utflen * 2];
        }

        int chararr_count = 0; // Position within the char array
        int b; // byte read
        int ch; // character read
        
        // Start with a loop expecting each character to be encoded by one byte
        // This will be most likely the case for most strings.
        while (count < endPos) {
            b = source[count]&0xff;
            if (!(b < 0xc0)) break; // Once a character is encountered which is encoded with multiple bytes, jump to the next loop
            count++;
            assert b < 0x80;
            ch = b;
            chararr_count = putChar(chararr_count, ch);
        }

        // Decode characters which can be encoded by multiple bytes
        while (count < endPos) {
            b = source[count++]&0xff;
            if (b < 0xc0) {
                assert b < 0x80;
                ch = b;
            } else if (b < 0xe0) {
                ch = ((b&0x1f)<<6) + (source[count++]&0x3f);
            } else if (b < 0xf0) {
                ch = ((b&0xf)<<12) + ((source[count++]&0x3f)<<6) + (source[count++]&0x3f);
            } else {
                assert b < 0xf8;
                ch = ((b&0x7)<<18) + ((source[count++]&0x3f)<<12) + ((source[count++]&0x3f)<<6) + (source[count++]&0x3f);
            }

            chararr_count = putChar(chararr_count, ch);
        }
        pos += utflen;
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
    
    private int putChar(int chararr_count, int ch) {
        if (ch <= UNI_MAX_BMP) {
            // target is a character <= 0xFFFF
            chararr[chararr_count++] = (char) ch;
        } else {
            // target is a character in range 0xFFFF - 0x10FFFF
            chararr[chararr_count++] = (char) ((ch >> HALF_SHIFT) + 0xD7C0 /* UNI_SUR_HIGH_START - 64 */);
            chararr[chararr_count++] = (char) ((ch & HALF_MASK) + UNI_SUR_LOW_START);
        }
        return chararr_count;
    }

    @Override
    public boolean readBoolean() {
        return (source[pos++] != 0);
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public int readInt() {
        return ((readByte() & 0xFF) << 24) 
            | ((readByte() & 0xFF) << 16)
            | ((readByte() & 0xFF) << 8) 
            | (readByte() & 0xFF);
    }

    @Override
    public long readLong() {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    @Override
    public int readShort() {
        return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }
    
    /**
    * Reads an int stored in variable-length format. Reads between one and
    * five bytes. Smaller values take fewer bytes. Negative numbers are not
    * supported.
    */
    @Override
    public int readVInt() {
        byte b = readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }
    
    /**
    * Reads a long stored in variable-length format. Reads between one and
    * nine bytes. Smaller values take fewer bytes. Negative numbers are not
    * supported.
    */
    @Override
    public long readVLong() {
        byte b = readByte();
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7FL) << shift;
        }
        return i;
    }
    
    @Override
    public int getPosition() {
        return pos;
    }
    
    @Override
    public void setPosition(int position) {
        this.pos = position;
    }
    
    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void setSize(int size) {
        if (size < 0 || size > source.length) {
            throw new IllegalArgumentException("Invalid size: " + size + " (maximum: " + source.length + ")");
        }
        this.size = size;
    }
}
