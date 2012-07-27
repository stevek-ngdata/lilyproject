package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;


public class FieldTemplatePart implements TemplatePart {

    private FieldType fieldType;

    private QName fieldName;

    public FieldTemplatePart(FieldType fieldType, QName fieldName) {
        this.fieldType = fieldType;
        this.fieldName = fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public QName getFieldName() {
        return fieldName;
    }
}
