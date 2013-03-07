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
package org.lilyproject.repository.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ArgumentValidator;

public class FieldTypesImpl implements FieldTypes {
    private Log log = LogFactory.getLog(getClass());

    // Always use getNameCache() instead of this variable directly, to make sure
    // this is the up-to-date nameCache (in case of FieldTypesCache).
    protected Map<QName, FieldType> nameCache;
    protected Map<String, Map<SchemaId, FieldType>> buckets;

    public FieldTypesImpl() {
        nameCache = new HashMap<QName, FieldType>();
        buckets = new ConcurrentHashMap<String, Map<SchemaId, FieldType>>();
    }

    protected Map<QName, FieldType> getNameCache() throws InterruptedException {
        return nameCache;
    }

    @Override
    public List<FieldType> getFieldTypes() throws InterruptedException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : getNameCache().values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }

    @Override
    public FieldType getFieldType(SchemaId id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        String bucket = AbstractSchemaCache.encodeHex(id.getBytes());

        Map<SchemaId, FieldType> fieldTypeIdCacheBucket = buckets.get(bucket);
        if (fieldTypeIdCacheBucket == null) {
            throw new FieldTypeNotFoundException(id);
        }
        FieldType fieldType = fieldTypeIdCacheBucket.get(id);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return fieldType.clone();
    }

    @Override
    public FieldType getFieldType(QName name) throws FieldTypeNotFoundException, InterruptedException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = getNameCache().get(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByNameReturnNull(QName name) throws InterruptedException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = getNameCache().get(name);
        return fieldType != null ? fieldType.clone() : null;
    }

    public boolean fieldTypeExists(QName name) throws InterruptedException {
        return getNameCache().containsKey(name);
    }
}
