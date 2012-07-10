package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;


public class FieldTemplatePart implements TemplatePart {

    private FieldType fieldType;

    public FieldTemplatePart(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

}
