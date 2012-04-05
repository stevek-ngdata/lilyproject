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
     * Same as {@link #readUTF()}, but for when the length
     * of the string was encoded as a variable-length integer,
     * thus a written by {@link DataOutput#writeVUTF}.
     */
    String readVUTF();
    
    /**
     * Reads a string from the <code>DataInput</code>
     */
    String readUTF(int length);

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

    /**
     * Allows to shrink (not grow) the input to make it appear shorter. Does not actually resize anything.
     */
    void setSize(int size);

    /**
     * Search for the first occurrence of the given byte, starting from the current position.
     * Does not modify the position.
     *
     * @return -1 if not found, otherwise the (absolute) position where it is found
     */
    int indexOf(byte value);
}
