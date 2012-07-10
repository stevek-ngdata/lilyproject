package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.QName;

public class FieldTemplatePart implements TemplatePart {

    private QName field;

    public FieldTemplatePart(QName field) {
        this.field = field;
    }

    public QName getField() {
        return field;
    }

}
