package org.lilyproject.bytes.api;

/**
 * The <code>DataOutput</code> converts primitive types to bytes
 * and writes them to an underlying byte array.
 * 
 * <p>The <code>DataOutput</code> maintains the position within the 
 * underlying byte[]. Every write call writes the value after the previous value.
 * 
 * <p>The underlying byte[] is automatically resized,
 * avoiding IOExceptions or OutOfBoundsExceptions.
 * 
 * <p>This array can be retrieved by calling toByteArray().
 * 
 * <p>A related {@link DataInput} should be created based on the byte[]
 * created by a <code>DataOutput</code>.
 */
public interface DataOutput {

    /**
     * Returns the underlying byte[]
     */
    byte[] toByteArray();
    
    /**
     * Writes a byte to the <code>DataOutput</code> 
     */
    void writeByte(byte b);

    /**
     * Writes a byte[] to the <code>DataOutput</code> 
     */
    void writeBytes(byte[] value);

    /**
     * Writes a string to the <code>DataOutput</code>
     * The string is encoded in unmodified UTF-8.
     * Its encoding includes the size of the string so that it can be read from a {@link DataInput} 
     * without the need to specify its size.
     */
    void writeUTF(String value);

    /**
     * Writes an integer to the <code>DataOutput</code>
     */
    void writeInt(int value);
    
    /**
     * Writes a long to the <code>DataOutput</code>
     */
    void writeLong(long value);
    
    /**
     * Writes a boolean to the <code>DataOutput</code>
     */
    void writeBoolean(boolean value);

    /**
     * Writes a double to the <code>DataOutput</code>
     */
    void writeDouble(double value);
    
    /**
     * Writes a short to the <code>DataOutput</code>
     */
    void writeShort(int value);

    /**
     * Writes a float to the <code>DataOutput</code>
     */
    void writeFloat(float v);

    /**
     * Writes an integer to the <code>DataOutput</code>
     * Its encoding will be variable length between 1 and 5 bytes.
     * Smaller values have smaller encodings. 
     * Negative numbers are not supported.
     */
    void writeVInt(int i);

    /**
     * Writes a long to the <code>DataOutput</code>
     * Its encoding will be variable length between 1 and 5 bytes.
     * Smaller values have smaller encodings. 
     * Negative numbers are not supported.
     */
    void writeVLong(long i);
    
    /**
     * Returns the current number of bytes in the <code>DataOutput</code>
     */
    int getSize();
}
