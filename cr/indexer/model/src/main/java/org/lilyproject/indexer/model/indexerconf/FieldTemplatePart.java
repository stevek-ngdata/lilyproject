package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;

public class FieldTemplatePart implements TemplatePart {

    private QName field;
    private SchemaId fieldId;

    public FieldTemplatePart(QName field, SchemaId fieldId) {
        this.field = field;
        this.fieldId = fieldId;
    }

    public QName getField() {
        return field;
    }

    public SchemaId getFieldId() {
        return fieldId;
    }

}
