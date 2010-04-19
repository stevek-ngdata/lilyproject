package org.lilycms.indexer.conf;

import java.util.Set;

import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Record.Scope;
import org.lilycms.repository.api.exception.FieldNotFoundException;

public class Value {
    private String fieldName;
    private final Scope scope;

    public Value(String fieldName, Scope scope) {
        this.fieldName = fieldName;
        this.scope = scope;
    }

    public String eval(Record record) {
        try {
                return (String)record.getField(scope, fieldName);
        } catch (FieldNotFoundException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    void collectFieldDependencies(Set<String> fields) {
        fields.add(fieldName);
    }
}
