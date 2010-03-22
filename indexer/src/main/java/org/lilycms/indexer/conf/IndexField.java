package org.lilycms.indexer.conf;

public class IndexField {
    private String name;
    private IndexFieldType type;
    private boolean indexed;
    private boolean stored;
    private boolean multiValued;
    private boolean omitNorms;
    private boolean termVectors;
    private boolean termPositions;
    private boolean termOffsets;
    private String defaultValue;

    public IndexField(String name, IndexFieldType type, boolean indexed, boolean stored, boolean multiValued,
            boolean omitNorms, boolean termVectors, boolean termPositions, boolean termOffsets, String defaultValue) {
        this.name = name;
        this.type = type;
        this.indexed = indexed;
        this.stored = stored;
        this.multiValued = multiValued;
        this.omitNorms = omitNorms;
        this.termVectors = termVectors;
        this.termPositions = termPositions;
        this.termOffsets = termOffsets;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public IndexFieldType getType() {
        return type;
    }

    public boolean getIndexed() {
        return indexed;
    }

    public boolean getStored() {
        return stored;
    }

    public boolean getMultiValued() {
        return multiValued;
    }

    public boolean getOmitNorms() {
        return omitNorms;
    }

    public boolean getTermVectors() {
        return termVectors;
    }

    public boolean getTermPositions() {
        return termPositions;
    }

    public boolean getTermOffsets() {
        return termOffsets;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
