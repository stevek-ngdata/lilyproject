package org.lilycms.indexer.conf;

import org.lilycms.repository.api.IdRecord;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.FieldNotFoundException;

import java.util.Collections;
import java.util.List;

public class FieldValue implements Value {
    private String fieldId;

    protected FieldValue(String fieldId) {
        this.fieldId = fieldId;
    }

    public List<String> eval(IdRecord record, Repository repository, String vtag) {
        try {
            return Collections.singletonList((String)record.getField(fieldId));
        } catch (FieldNotFoundException e) {
            // TODO
            return null;
        }
    }

    public String getFieldDependency() {
        return fieldId;
    }

}
