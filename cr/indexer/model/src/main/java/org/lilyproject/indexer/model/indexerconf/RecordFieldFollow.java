package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;

public class RecordFieldFollow implements Follow {
    private FieldType fieldType;

    public RecordFieldFollow(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }
}
