package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.SchemaId;

/**
 * For describing information about a record type or a field type.
 */
public class Type<T> {
    protected SchemaId id;
    protected Long version;
    protected T object;

    public SchemaId getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public T getObject() {
        return object;
    }
}
