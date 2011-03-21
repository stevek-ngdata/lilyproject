package org.lilyproject.bytes.api;

/**
 * <code>DataInput</code> reads primitive types from a byte[] which has been encoded using the {@link DataOutput}
 * 
 * <p>Every read call reads the next value from the underlying byte[].
 */
public interface DataInput {

    /**
     * Reads <code>length</code> number of bytes from the <code>DataInput</code>
     */
    byte[] readBytes(int length);
    
    /**
     * Reads one byte from the <code>DataInput</code>
     */
    byte readByte();
    
    /**
     * Reads an integer from the <code>DataInput</code>
     */
    int readInt();
    
    /**
     * Reads a long from the <code>DataInput</code>
     */
    long readLong();
    
    /**
     * Reads a string from the <code>DataInput</code>
     * The length of the string has been encoded by the {@link DataOutput},
     * so no length needs to be given as parameter. 
     */
    String readUTF();

    /**
     * Reads a boolean from the <code>DataInput</code>
     */
    boolean readBoolean();

    /**
     * Reads a double from the <code>DataInput</code>
     */
    double readDouble();
    
    /**
     * Reads a short from the <code>DataInput</code>
     */
    int readShort();

    /**
     * Reads a float from the <code>DataInput</code>
     */
    float readFloat();

    /**
     * Reads a integer from the <code>DataInput</code>
     * which has been encoded with a variable number of bytes
     * by {@link DataOutput#writeVInt(int)}
     */
    int readVInt();

    /**
     * Reads a long from the <code>DataInput</code>
     * which has been encoded with a variable number of bytes
     * by {@link DataOutput#writeVLong(long)}
     */
    long readVLong();
    
    /**
     * Returns the position in the <code>DataInput</code> from where the next value will be read.
     */
    int getPosition();
    
    /**
     * Sets the position in the <code>DataInput</code> from where to read the next value.
     */
    void setPosition(int position);
    
    /**
     * Returns the total number of bytes in the <code>DataInput</code>
     */
    int getSize();
}
