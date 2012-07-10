package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;

public class LinkFieldFollow implements Follow {
    private FieldType fieldType;
    /**
     * If the link field follow is after one or more record follows, then from
     * the point of view of the link index, the link belongs to the same field
     * as the top-level record field. We keep a reference to that field here.
     */
    private FieldType ownerFieldType;

    public LinkFieldFollow(FieldType fieldType) {
        this.fieldType = fieldType;
        this.ownerFieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public FieldType getOwnerFieldType() {
        return ownerFieldType;
    }

    public void setOwnerFieldType(FieldType ownerFieldType) {
        this.ownerFieldType = ownerFieldType;
    }

}
