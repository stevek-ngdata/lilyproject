package org.lilycms.hbaseindex;

public abstract class IndexFieldDefinition {
    private String name;
    private IndexValueType type;

    public IndexFieldDefinition(String name, IndexValueType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public IndexValueType getType() {
        return IndexValueType.INTEGER;
    }

    /**
     * The number of bytes this entry takes in the index row key.
     */
    public abstract int getByteLength();

    public abstract int toBytes(byte[] bytes, int offset, Object value);

}
