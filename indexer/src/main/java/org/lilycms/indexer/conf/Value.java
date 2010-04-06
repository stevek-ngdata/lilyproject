package org.lilycms.indexer.conf;

import java.util.Set;

import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;

public class Value {
    private String fieldName;
    private final boolean versioned;

    public Value(String fieldName, boolean versioned) {
        this.fieldName = fieldName;
        this.versioned = versioned;
    }

    public String eval(Record record) {
        try {
            if (versioned) {
                return (String)record.getVersionableField(fieldName);
            } else {
                return (String)record.getNonVersionableField(fieldName);
            }
        } catch (FieldNotFoundException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    void collectFieldDependencies(Set<String> fields) {
        fields.add(fieldName);
    }
}
