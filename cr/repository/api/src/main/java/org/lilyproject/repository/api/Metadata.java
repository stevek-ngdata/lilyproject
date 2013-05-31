/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.api;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.util.ObjectUtils;

/**
 * A set of free, typed key-value pairs.
 *
 * <p>Metadata objects can only be created using the {@link MetadataBuilder} and are immutable after creation.</p>
 *
 * <p>This metadata can be associated with record field values, see {@link Record#setMetadata(QName, Metadata)}.</p>
 */
public class Metadata implements Cloneable {
    private Map<String, Object> data;
    private Set<String> fieldsToDelete;

    protected Metadata(Map<String, Object> data, Set<String> fieldsToDelete) {
        this.data = Collections.unmodifiableMap(data);
        this.fieldsToDelete = Collections.unmodifiableSet(fieldsToDelete);
    }

    public String get(String key) {
        return data.get(key).toString();
    }

    /**
     * Returns the metadata value without any type coercion.
     */
    public Object getObject(String key) {
        return data.get(key);
    }

    public Integer getInt(String key, Integer defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number) {
            return ((Number)value).intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    public Long getLong(String key, Long defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number) {
            return ((Number)value).longValue();
        } else {
            return Long.parseLong(value.toString());
        }
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Boolean) {
            return (Boolean)value;
        } else {
            throw new IllegalStateException("Value is not a boolean for metadata field '" + key + "': "
                    + value.getClass().getName());
        }
    }

    public Float getFloat(String key, Float defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number) {
            return ((Number)value).floatValue();
        } else {
            return Float.parseFloat(value.toString());
        }
    }

    public Double getDouble(String key, Double defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number) {
            return ((Number)value).doubleValue();
        } else {
            return Double.parseDouble(value.toString());
        }
    }

    public ByteArray getBytes(String key) {
        Object value = data.get(key);
        if (value instanceof ByteArray) {
            return (ByteArray)value;
        } else {
            throw new IllegalStateException("Value is not a byte array for metadata field '" + key + "': "
                    + value.getClass().getName());
        }
    }

    public DateTime getDateTime(String key, DateTime defaultValue) {
        Object value = data.get(key);
        if (value == null) {
            return defaultValue;
        } else if (value instanceof DateTime) {
            return (DateTime)value;
        } else {
            throw new IllegalStateException("Value is not a DateTime for metadata field '" + key + "': "
                    + value.getClass().getName());
        }
    }

    public boolean contains(String key) {
        return data.containsKey(key);
    }

    /**
     * Checks there are no metadata fields and that the fields-to-delete is empty.
     *
     * <p>If you only want to know if there are no fields (ignoring fieldsToDelete), use
     * {@link #getMap()}.isEmtpy().</p>
     */
    public boolean isEmpty() {
        return data.isEmpty() && fieldsToDelete.isEmpty();
    }

    public Map<String, Object> getMap() {
        return data;
    }

    public Set<String> getFieldsToDelete() {
        return fieldsToDelete;
    }

    /**
     * Tests if this metadata will change the existing metadata on a field.
     *
     * <p>This is different from a normal equals, since it looks at the effect of the
     * {@link #getFieldsToDelete() fields to delete}, rather than comparing the lists
     * of fields to delete.</p>
     *
     * <p>This method can e.g. be used to set a metadata field only if there are already
     * metadata changes, or to figure out if a record changed compared to a previous state.</p>
     *
     * @param oldMetadata the current metadata on a field. This can be null, in which case this
     *                    method will always return true.
     */
    public boolean updates(Metadata oldMetadata) {
        if (oldMetadata == null) {
            return true;
        }

        // Metadata has not changed if:
        //   - all KV's in the new metadata are also in the old metadata
        //   - any deletes in the new metadata refer to fields that didn't exist in the old metadata

        for (Map.Entry<String, Object> entry : this.getMap().entrySet()) {
            Object oldValue = oldMetadata.getObject(entry.getKey());
            if (!ObjectUtils.safeEquals(oldValue, entry.getValue())) {
                return true;
            }
        }

        for (String key : this.getFieldsToDelete()) {
            if (oldMetadata.contains(key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "data=" + data +
                ", fieldsToDelete=" + fieldsToDelete +
                '}';
    }
}
