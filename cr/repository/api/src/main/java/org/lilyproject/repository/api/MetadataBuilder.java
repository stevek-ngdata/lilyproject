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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.lilyproject.bytes.api.ByteArray;

/**
 * Builder for {@link Metadata} objects.
 *
 * <p>The way this works is that you instantiate a MetadataBuilder, call a few of its {@link #value(String, String)}
 * methods, and finally call {@link #build()} to create a Metadata object</p>
 *
 * <p>An example:</p>
 *
 * <pre>
 *     Metadata metadata = new MetadataBuilder()
 *         .value("field1", "value1")
 *         .value("field2", 5)
 *         .build();
 * </pre>
 */
public class MetadataBuilder {
    private Map<String, Object> data = new HashMap<String, Object>();
    private Set<String> fieldsToDelete;

    public MetadataBuilder value(String key, String value) {
        if (value == null) {
            delete(key);
        } else {
            data.put(key, value);
            removeFromFieldsToDelete(key);
        }
        return this;
    }

    /**
     * Generic setter for all types of values.
     *
     * <p>The actual types of values supported are still only those for which there are
     * specific setters as well.</p>
     */
    public MetadataBuilder object(String key, Object value) {
        if (value == null) {
            delete(key);
        } else {
            data.put(key, value);
            removeFromFieldsToDelete(key);
        }
        return this;
    }

    public MetadataBuilder value(String key, int value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    public MetadataBuilder value(String key, long value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    public MetadataBuilder value(String key, boolean value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    public MetadataBuilder value(String key, float value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    public MetadataBuilder value(String key, double value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    public MetadataBuilder value(String key, ByteArray value) {
        if (value == null) {
            delete(key);
        } else {
            data.put(key, value);
            removeFromFieldsToDelete(key);
        }
        return this;
    }

    public MetadataBuilder value(String key, DateTime value) {
        data.put(key, value);
        removeFromFieldsToDelete(key);
        return this;
    }

    /**
     * Explicitly deletes a metadata value, adding it to the list of fields to delete.
     */
    public MetadataBuilder delete(String key) {
        data.remove(key);
        if (fieldsToDelete == null) {
            fieldsToDelete = new HashSet<String>();
        }
        fieldsToDelete.add(key);
        return this;
    }

    private void removeFromFieldsToDelete(String field) {
        if (fieldsToDelete != null) {
            fieldsToDelete.remove(field);
        }
    }

    public Metadata build() {
        // because builders could be reused, and we want metadata to be immutable, the map & set are cloned,
        // though I don't like the extra work & garbage this creates (alternatives would be to make builders
        // non-reusable or to let the build method also do a reset of the builder state)
        return new Metadata(new HashMap<String, Object>(data),
                fieldsToDelete == null ? Collections.<String>emptySet() : new HashSet<String>(fieldsToDelete));
    }
}
