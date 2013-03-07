/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.*;

import java.util.*;

public class Fields {
    Map<Long, Map<SchemaId, Object>> values = new HashMap<Long, Map<SchemaId, Object>>();
    List<FieldType> fields = new ArrayList<FieldType>();

    public static Object DELETED = new Object();

    public FieldType registerFieldType(SchemaId fieldId, TypeManager typeMgr) throws Exception {
        for (FieldType entry : fields) {
            if (entry.getId().equals(fieldId)) {
                return entry;
            }
        }

        FieldType fieldType = typeMgr.getFieldTypeById(fieldId);

        fields.add(fieldType);
        return fieldType;
    }

    public List<FieldType> getFieldTypes() {
        return fields;
    }

    public Object getValue(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        if (valuesByColumn == null) {
            return null;
        }
        return valuesByColumn.get(fieldId);
    }

    public boolean isNull(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        return valuesByColumn == null || !valuesByColumn.containsKey(fieldId);
    }

    public boolean isDeleted(long version, SchemaId fieldId) {
        Map<SchemaId, Object> valuesByColumn = values.get(version);
        return valuesByColumn != null && valuesByColumn.get(fieldId) == DELETED;
    }

    public void collectVersions(Set<Long> versions) {
        versions.addAll(values.keySet());
    }
}
