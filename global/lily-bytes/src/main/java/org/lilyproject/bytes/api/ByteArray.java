/*
 * Copyright 2011 Outerthought bvba
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

import java.util.Arrays;

/**
 * A utility class providing read only access to a byte[].
 * <p/>
 * Upon construction, the byte[] to be wrapped needs to be given.</br>
 * <p/>
 * This byte[] is copied by the constructor {@link ByteArray#ByteArray(byte[])}
 * to avoid external changes to the byte[] from having an impact.
 * <p/>
 * If the user is absolutely certain a byte[] will not be modified later on, the
 * factory method {@link ByteArray#wrap(byte[])} can be used which will not copy
 * the byte[] but just wrap it.
 */
public class ByteArray {

    private byte[] bytes;

    /**
     * Provides an uninitialized ByteArray.
     * <p/>
     * For internal use only.
     */
    private ByteArray() {
    }

    /**
     * Creates a ByteArray wrapper around a byte[] which is first copied.
     */
    public ByteArray(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Wraps a byte[] instead of copying it like the constructor does.
     * <p/>
     * Warning! This method should only be used when the user is absolutely
     * certain that the provided byte[] will not be modified anymore.
     *
     * @param bytes the byte[] to wrap
     * @return a ByteArray
     */
    public static ByteArray wrap(byte[] bytes) {
        ByteArray byteArray = new ByteArray();
        byteArray.bytes = bytes;
        return byteArray;
    }

    /**
     * Returns a copy of the wrapped byte[].
     */
    public byte[] getBytes() {
        return Arrays.copyOf(this.bytes, this.bytes.length);
    }

    /**
     * Returns the wrapped byte[] without copying it first.
     * <p/>
     * Warning! This method should only be used when the user is absolutely
     * certain that the returned byte[] will nog be modified anymore.
     */
    public byte[] getBytesUnsafe() {
        return bytes;
    }

    /**
     * Returns the byte at index of the wrapped byte[].
     *
     * @param index of the byte to return
     * @return a byte
     * @throws ArrayIndexOutOfBoundsException when the given index is outside the range of the wrapped
     *                                        byte[]
     */
    public byte get(int index) {
        return this.bytes[index];
    }

    /**
     * Returns the length of the wrapped byte[]
     */
    public int length() {
        return this.bytes.length;
    }

    /**
     * Returns a copy of the specified range of the wrapped byte[]
     *
     * @param from the initial index of the range to be copied, inclusive
     * @param to   the final index of the range to be copied, exclusive. (This
     *             index may lie outside the array in which case it is limited to
     *             the last index of the original byte[].)
     */
    public byte[] getRange(int from, int to) {
        return Arrays.copyOfRange(this.bytes, from, to);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ByteArray other = (ByteArray)obj;
        if (!Arrays.equals(bytes, other.bytes)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "[ByteArray, length=" + bytes.length + "]";
    }
}
