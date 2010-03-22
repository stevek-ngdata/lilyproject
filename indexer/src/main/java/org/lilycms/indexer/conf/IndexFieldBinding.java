package org.lilycms.indexer.conf;

/**
 * Binds an index field to a value.
 */
public class IndexFieldBinding {
    private IndexField field;
    private Value value;

    public IndexFieldBinding(IndexField field, Value value) {
        this.field = field;
        this.value = value;
    }

    public IndexField getField() {
        return field;
    }

    public Value getValue() {
        return value;
    }
}
