package org.lilycms.indexer.conf;

import java.util.Set;

import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.exception.FieldNotFoundException;

public class Value {
    private QName fieldName;

    public Value(QName fieldName) {
        this.fieldName = fieldName;
    }

    public String eval(Record record) {
        try {
            return (String)record.getField(fieldName);
        } catch (FieldNotFoundException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    void collectFieldDependencies(Set<QName> fields) {
        fields.add(fieldName);
    }
}
