package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdRecord;

/**
 * A value to index, together with the coordinates where it came from.
 */
public class IndexValue {
    public IdRecord record;
    public FieldType fieldType;
    public Integer listIndex;
    public Object value;

    public IndexValue(IdRecord record, FieldType fieldType, Integer listIndex, Object value) {
        this.record = record;
        this.fieldType = fieldType;
        this.listIndex = listIndex;
        this.value = value;
    }

    public IndexValue(IdRecord record, FieldType fieldType, Object value) {
        this(record, fieldType, null, value);
    }
}
