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
package org.lilyproject.hbaseindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lilyproject.util.ByteArrayKey;

/**
 * An entry to add to or remove from an Index.
 *
 * <p>This object can be simply instantiated yourself and passed to
 * {@link Index#addEntry} or {@link Index#removeEntry}.
 *
 * <p>The fields added to this entry should correspond in name
 * and type to the fields defined in the {@link IndexDefinition}.
 *
 * <p>Missing fields will be interpreted as fields with a null value.
 */
public class IndexEntry {
    private final IndexDefinition definition;
    private Map<String, Object> fields = new HashMap<String, Object>();
    private Map<ByteArrayKey, byte[]> data = new HashMap<ByteArrayKey, byte[]>();
    private byte[] identifier;

    public IndexEntry(IndexDefinition definition) {
        this.definition = definition;
    }

    public IndexEntry(IndexDefinition definition, byte[] identifier) {
        this.definition = definition;
        this.identifier = identifier;
    }

    public void addField(String name, Object value) {
        definition.checkFieldSupport(name, value);

        fields.put(name, value);
    }

    /**
     * Allows to add optional data to be stored in the index entry row.
     *
     * @param qualifier
     * @param value
     */
    public void addData(byte[] qualifier, byte[] value) {
        data.put(new ByteArrayKey(qualifier), value);
    }

    protected Map<ByteArrayKey, byte[]> getData() {
        return data;
    }

    /**
     * Get the values to be serialized into a byte array, in index definition order. Missing fields are inserted as
     * null
     * values.
     *
     * @return the values to be serialized into a byte array
     */
    Object[] getFieldValuesInSerializationOrder() {
        final List<Object> values = new ArrayList<Object>(definition.getFields().size() + 1);
        for (IndexFieldDefinition indexFieldDefinition : definition.getFields()) {
            final Object fieldValueOrNull = fields.get(indexFieldDefinition.getName());
            values.add(fieldValueOrNull);
        }

        values.add(identifier);

        return values.toArray();
    }

    public void setIdentifier(byte[] identifier) {
        this.identifier = identifier;
    }

    public void validate() {
        if (identifier == null) {
            throw new MalformedIndexEntryException("Index entry does not specify an identifier.");
        }
    }
}
