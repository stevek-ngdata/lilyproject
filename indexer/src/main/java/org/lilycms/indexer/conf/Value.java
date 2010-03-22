package org.lilycms.indexer.conf;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;

public class Value {
    private String fieldName;

    public Value(String fieldName) {
        this.fieldName = fieldName;
    }

    public String eval(Record record) {
        try {
            return Bytes.toString(record.getField(fieldName).getValue());
        } catch (FieldNotFoundException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }
}
