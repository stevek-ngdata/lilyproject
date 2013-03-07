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
package org.lilyproject.linkindex;

import java.util.List;
import java.util.Map;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.ValueType;

public class RecordLinkExtractor {
    private RecordLinkExtractor() {
    }

    /**
     * Extracts the links from a record. The provided Record object should
     * be "fully loaded" (= contain all fields).
     */
    public static void extract(IdRecord record, LinkCollector collector, RepositoryManager repositoryManager)
            throws RepositoryException, InterruptedException {
        for (Map.Entry<SchemaId, Object> field : record.getFieldsById().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = repositoryManager.getTypeManager().getFieldTypeById(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // Can not do anything with a field if we cannot load its type
                continue;
            }
            extract(field.getValue(), fieldType, collector, fieldType, record.getId(), repositoryManager);
        }
    }

    /**
     * This is for link extraction from nested records.
     */
    private static void extractRecord(Record record, LinkCollector collector, FieldType ctxField, RecordId ctxRecord,
            RepositoryManager repositoryManager)
            throws RepositoryException, InterruptedException {
        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = repositoryManager.getTypeManager().getFieldTypeByName(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // Can not do anything with a field if we cannot load its type
                continue;
            }

            // The ctxField and ctxRecord need to stay the top-level ones! It does not matter how
            // deeply nested a link occurs, as far as the link index is concerned, it still occurs
            // with the field of the top level record.
            extract(field.getValue(), fieldType, collector, ctxField, ctxRecord, repositoryManager);
        }
    }

    private static void extract(Object value, FieldType fieldType, LinkCollector collector, FieldType ctxField,
            RecordId ctxRecord, RepositoryManager repositoryManager) throws RepositoryException, InterruptedException {

        ValueType valueType = fieldType.getValueType();

        String baseType = valueType.getDeepestValueType().getBaseName();

        if (baseType.equals("LINK") || baseType.equals("RECORD")) {
            extract(value, collector, ctxField, ctxRecord, repositoryManager);
        }
    }

    private static void extract(Object value, LinkCollector collector, FieldType ctxField, RecordId ctxRecord,
            RepositoryManager repositoryManager) throws RepositoryException, InterruptedException {

        if (value instanceof List) {
            List list = (List)value;
            for (Object item : list) {
                extract(item, collector, ctxField, ctxRecord, repositoryManager);
            }
        } else if (value instanceof Record) {
            extractRecord((Record)value, collector, ctxField, ctxRecord, repositoryManager);
        } else if (value instanceof Link) {
            RecordId recordId = ((Link)value).resolve(ctxRecord, repositoryManager.getIdGenerator());
            collector.addLink(recordId, ctxField.getId());
        } else {
            throw new RuntimeException("Encountered an unexpected kind of object from a link field: " +
                    value.getClass().getName());
        }
    }
}
