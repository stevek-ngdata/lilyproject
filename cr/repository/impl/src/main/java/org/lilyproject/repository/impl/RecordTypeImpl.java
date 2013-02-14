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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.ArgumentValidator;

public class RecordTypeImpl implements RecordType {

    private final SchemaId id;
    private final QName name;
    private final Long version;
    private final Map<SchemaId, Long> mixins;
    private final Map<SchemaId, FieldTypeEntry> fieldTypeEntries;

    private static final ByteArrayComparator byteArrayComparator = new ByteArrayComparator();

    private static final Comparator<SchemaId> comparator = new Comparator<SchemaId>() {
        @Override
        public int compare(SchemaId schemaId1, SchemaId schemaId2) {
            return byteArrayComparator.compare(schemaId1.getBytes(), schemaId2.getBytes());
        }
    };

    /**
     * This constructor should not be called directly.
     *
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(SchemaId id, QName name) {
        this(id, name, null, Collections.<SchemaId, Long>emptyMap(), Collections.<FieldTypeEntry>emptySet());
    }

    /**
     * This constructor should not be called directly.
     *
     * @use {@link TypeManager#newRecordType} instead
     */
    public RecordTypeImpl(SchemaId id, QName name, Long version) {
        this(id, name, version, Collections.<SchemaId, Long>emptyMap(), Collections.<FieldTypeEntry>emptySet());
    }

    RecordTypeImpl(SchemaId id, QName name, Long version, Map<SchemaId, Long> mixins,
            Collection<FieldTypeEntry> fieldTypeEntries) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.mixins = Collections.unmodifiableMap(mixins);

        // Sort the fieldTypeEntries at creation time so we don't have to sort them
        // each time they are requested.
        final TreeMap<SchemaId, FieldTypeEntry> mutableFieldTypeEntries =
                new TreeMap<SchemaId, FieldTypeEntry>(comparator);
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            mutableFieldTypeEntries.put(fieldTypeEntry.getFieldTypeId(), fieldTypeEntry);
        }
        this.fieldTypeEntries = Collections.unmodifiableMap(mutableFieldTypeEntries);
    }

    private Map<SchemaId, FieldTypeEntry> mutableFieldTypeEntries() {
        return new HashMap<SchemaId, FieldTypeEntry>(fieldTypeEntries);
    }

    private Map<SchemaId, Long> mutableMixins() {
        return new HashMap<SchemaId, Long>(mixins);
    }

    @Override
    public SchemaId getId() {
        return id;
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
    public Collection<FieldTypeEntry> getFieldTypeEntries() {
        return fieldTypeEntries.values();
    }

    @Override
    public FieldTypeEntry getFieldTypeEntry(SchemaId fieldTypeId) {
        return fieldTypeEntries.get(fieldTypeId);
    }

    @Override
    public RecordType withoutFieldTypeEntry(SchemaId fieldTypeId) {
        final Map<SchemaId, FieldTypeEntry> mutableFieldTypeEntries = mutableFieldTypeEntries();
        mutableFieldTypeEntries.remove(fieldTypeId);

        return new RecordTypeImpl(this.id, this.name, this.version, this.mixins, mutableFieldTypeEntries.values());
    }

    @Override
    public RecordType withFieldTypeEntry(FieldTypeEntry fieldTypeEntry) {
        final Map<SchemaId, FieldTypeEntry> mutableFieldTypeEntries = mutableFieldTypeEntries();
        mutableFieldTypeEntries.put(fieldTypeEntry.getFieldTypeId(), fieldTypeEntry);

        return new RecordTypeImpl(this.id, this.name, this.version, this.mixins, mutableFieldTypeEntries.values());
    }

    @Override
    public RecordType withFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory) {
        return withFieldTypeEntry(new FieldTypeEntryImpl(fieldTypeId, mandatory));
    }

    @Override
    public RecordType withMixin(SchemaId recordTypeId, Long recordTypeVersion) {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");
        final Map<SchemaId, Long> mutableMixins = mutableMixins();
        mutableMixins.put(recordTypeId, recordTypeVersion);

        return new RecordTypeImpl(this.id, this.name, this.version, mutableMixins, this.fieldTypeEntries.values());
    }

    @Override
    public RecordType withMixin(SchemaId recordTypeId) {
        return withMixin(recordTypeId, null);
    }

    @Override
    public RecordType withoutMixin(SchemaId recordTypeId) {
        final Map<SchemaId, Long> mutableMixins = mutableMixins();
        mutableMixins.remove(recordTypeId);

        return new RecordTypeImpl(this.id, this.name, this.version, mutableMixins, this.fieldTypeEntries.values());
    }

    @Override
    public Map<SchemaId, Long> getMixins() {
        return mixins;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldTypeEntries == null) ? 0 : fieldTypeEntries.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((mixins == null) ? 0 : mixins.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecordTypeImpl other = (RecordTypeImpl) obj;
        if (fieldTypeEntries == null) {
            if (other.fieldTypeEntries != null)
                return false;
        } else if (!fieldTypeEntries.equals(other.fieldTypeEntries))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (mixins == null) {
            if (other.mixins != null)
                return false;
        } else if (!mixins.equals(other.mixins))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RecordTypeImpl [name=" + name + ", id=" + id + ", version=" + version + ", fieldTypeEntries="
                + fieldTypeEntries + ", mixins=" + mixins + "]";
    }

}
