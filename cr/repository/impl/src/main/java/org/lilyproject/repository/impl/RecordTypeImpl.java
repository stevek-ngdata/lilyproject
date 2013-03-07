/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.ArgumentValidator;

public class RecordTypeImpl implements RecordType, Cloneable {

    private SchemaId id;
    private QName name;
    private Long version;
    private Map<SchemaId, Long> supertypes = new HashMap<SchemaId, Long>();
    private Map<SchemaId, FieldTypeEntry> fieldTypeEntries;
    private static final ByteArrayComparator byteArrayComparator = new ByteArrayComparator();
    private static final Comparator<SchemaId> comparator = new Comparator<SchemaId>() {
        @Override
        public int compare(SchemaId schemaId1, SchemaId schemaId2) {
            return byteArrayComparator.compare(schemaId1.getBytes(), schemaId2.getBytes());
        }
    };

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(SchemaId id, QName name) {
        this.id = id;
        this.name = name;
        // Sort the fieldTypeEntries at creation time so we don't have to sort them
        // each time they are requested.
        fieldTypeEntries = new TreeMap<SchemaId, FieldTypeEntry>(comparator);
    }

    @Override
    public void setId(SchemaId id) {
        this.id = id;
    }

    @Override
    public SchemaId getId() {
        return id;
    }

    @Override
    public void setName(QName name) {
        this.name = name;
    }

    @Override
    public QName getName() {
        return name;
    }

    @Override
    public Long getVersion() {
        return version;
    }

    @Override
    public void setVersion(Long version){
        this.version = version;
    }

    @Override
    public Collection<FieldTypeEntry> getFieldTypeEntries() {
        return fieldTypeEntries.values();
    }

    @Override
    public FieldTypeEntry getFieldTypeEntry(SchemaId fieldTypeId) {
        return fieldTypeEntries.get(fieldTypeId);
    }

    @Override
    public void removeFieldTypeEntry(SchemaId fieldTypeId) {
        fieldTypeEntries.remove(fieldTypeId);
    }

    @Override
    public void addFieldTypeEntry(FieldTypeEntry fieldTypeEntry) {
        fieldTypeEntries.put(fieldTypeEntry.getFieldTypeId(), fieldTypeEntry);
    }

    @Override
    public FieldTypeEntry addFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory) {
        FieldTypeEntry fieldTypeEntry = new FieldTypeEntryImpl(fieldTypeId, mandatory);
        addFieldTypeEntry(fieldTypeEntry);
        return fieldTypeEntry;
    }

    @Override
    public void addSupertype(SchemaId recordTypeId, Long recordTypeVersion) {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        supertypes.put(recordTypeId, recordTypeVersion);
    }

    @Override
    public void addSupertype(SchemaId recordTypeId) {
        addSupertype(recordTypeId, null);
    }

    @Override
    public void removeSupertype(SchemaId recordTypeId) {
        supertypes.remove(recordTypeId);
    }

    @Override
    public Map<SchemaId, Long> getSupertypes() {
        return supertypes;
    }

    @Override
    @Deprecated
    public void addMixin(SchemaId recordTypeId, Long recordTypeVersion) {
        addSupertype(recordTypeId, recordTypeVersion);
    }

    @Override
    @Deprecated
    public void addMixin(SchemaId recordTypeId) {
        addSupertype(recordTypeId, null);
    }

    @Override
    @Deprecated
    public void removeMixin(SchemaId recordTypeId) {
        removeSupertype(recordTypeId);
    }

    @Override
    @Deprecated
    public Map<SchemaId, Long> getMixins() {
        return supertypes;
    }

    @Override
    public RecordType clone() {
        RecordTypeImpl clone = new RecordTypeImpl(this.id, this.name);
        clone.version = this.version;
        clone.fieldTypeEntries.putAll(fieldTypeEntries);
        clone.supertypes.putAll(supertypes);
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldTypeEntries == null) ? 0 : fieldTypeEntries.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((supertypes == null) ? 0 : supertypes.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RecordTypeImpl other = (RecordTypeImpl) obj;
        if (fieldTypeEntries == null) {
            if (other.fieldTypeEntries != null) {
                return false;
            }
        } else if (!fieldTypeEntries.equals(other.fieldTypeEntries)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (supertypes == null) {
            if (other.supertypes != null) {
                return false;
            }
        } else if (!supertypes.equals(other.supertypes)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (version == null) {
            if (other.version != null) {
                return false;
            }
        } else if (!version.equals(other.version)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RecordTypeImpl [name=" + name + ", id=" + id + ", version=" + version + ", fieldTypeEntries="
                + fieldTypeEntries + ", supertypes=" + supertypes + "]";
    }

}
