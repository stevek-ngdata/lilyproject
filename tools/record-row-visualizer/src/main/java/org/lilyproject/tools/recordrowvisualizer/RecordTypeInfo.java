package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;

public class RecordTypeInfo {
    protected SchemaId id;
    protected QName name;

    public RecordTypeInfo(SchemaId id, QName name) {
        this.id = id;
        this.name = name;
    }

    public SchemaId getId() {
        return id;
    }

    public QName getName() {
        return name;
    }

    @Override
    public String toString() {
        return id + " " + name;
    }
}
