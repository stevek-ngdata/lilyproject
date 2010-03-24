package org.lilycms.indexer.conf;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;

import java.util.Set;

public class Value {
    private String fieldName;

    public Value(String fieldName) {
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

    void collectFieldDependencies(Set<String> fields) {
        fields.add(fieldName);
    }
}
