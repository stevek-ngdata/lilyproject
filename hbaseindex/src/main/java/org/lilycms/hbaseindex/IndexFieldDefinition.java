package org.lilycms.hbaseindex;

public abstract class IndexFieldDefinition {
    private final String name;
    private IndexValueType type;

    public IndexFieldDefinition(String name, IndexValueType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public IndexValueType getType() {
        return type;
    }

    /**
     * The number of bytes this entry takes in the index row key.
     */
    public abstract int getByteLength();

    /**
     * Same as the other toBytes method, with fillFieldLength = true.
     */
    public abstract int toBytes(byte[] bytes, int offset, Object value);

    /**
     * Converts the specified value to bytes according to the rules of this
     * IndexFieldDefinition.
     *
     * @param bytes the byte array into which the bytes should be added. The byte array
     *              should be large enough to store {@link #getByteLength()} bytes after the
     *              offset.
     * @param offset the offset at which the bytes should be added
     * @param value the value, assumed to be of the correct type
     * @param fillFieldLength if true, the bytes will be padded up to {@link #getByteLength()},
     *                        and the returned offset will hence be located after this length.
     *                        If false, the returned offset will only be after the actual
     *                        value length. Note that data types like number always use the
     *                        same length, this is mainly intended for strings.
     * @return the offset after the written data, thus where the next data could be written
     */
    public abstract int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength);
}
