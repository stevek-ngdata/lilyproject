package org.lilycms.indexer.conf;

import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.exception.FieldNotFoundException;

import java.util.Collections;
import java.util.List;

public class FieldValue implements Value {
    private QName fieldName;

    protected FieldValue(QName fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> eval(Record record, Repository repository, String vtag) {
        try {
            return Collections.singletonList((String)record.getField(fieldName));
        } catch (FieldNotFoundException e) {
            // TODO
            return null;
        }
    }

    public QName getFieldDependency() {
        return fieldName;
    }

}
