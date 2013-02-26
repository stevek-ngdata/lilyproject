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

import org.lilyproject.bytes.api.ByteArray;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

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
        return (String)data.get(key);
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

    public boolean contains(String key) {
        return data.containsKey(key);
    }

    public boolean isEmpty() {
        return data.isEmpty() && fieldsToDelete.isEmpty();
    }

    public Map<String, Object> getMap() {
        return data;
    }

    public Set<String> getFieldsToDelete() {
        return fieldsToDelete;
    }
}
