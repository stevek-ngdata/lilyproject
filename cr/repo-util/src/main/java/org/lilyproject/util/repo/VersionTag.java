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
package org.lilyproject.util.repo;

import org.lilyproject.repository.api.*;

import java.util.*;

/**
 * Version tag related utilities.
 */
public class VersionTag {

    /**
     * Namespace for field types that serve as version tags.
     */
    public static final String NAMESPACE = "org.lilyproject.vtag";

    /**
     * Name for the field type that serves as last version tag.
     */
    public static final QName LAST = new QName(NAMESPACE, "last");

    public static QName qname(String vtag) {
        return new QName(NAMESPACE, vtag);
    }

    /**
     * Returns true if the given FieldType is a version tag.
     */
    public static boolean isVersionTag(FieldType fieldType) {
        String namespace = fieldType.getName().getNamespace();
        return (namespace != null && namespace.equals(NAMESPACE)
                && fieldType.getScope() == Scope.NON_VERSIONED
                && fieldType.getValueType().getName().equals("LONG")
                && !fieldType.getName().getName().equals("last")); /* filter out 'last' vtag, it should not be
                                                                      custom assigned */
    }

    /**
     * Filters the given set of fields to only those that are vtag fields.
     */
    public static Set<SchemaId> filterVTagFields(Set<SchemaId> fieldIds, TypeManager typeManager)
            throws RepositoryException, InterruptedException {
        Set<SchemaId> result = new HashSet<SchemaId>();
        for (SchemaId field : fieldIds) {
            try {
                if (isVersionTag(typeManager.getFieldTypeById(field))) {
                    result.add(field);
                }
            } catch (FieldTypeNotFoundException e) {
                // ignore, if it does not exist, it can't be a version tag
            }
        }
        return result;
    }

    public static IdRecord getIdRecord(RecordId recordId, SchemaId vtagId, Repository repository)
            throws RepositoryException, InterruptedException {

        VTaggedRecord vtRecord = new VTaggedRecord(recordId, null, repository);
        return vtRecord.getIdRecord(vtagId);
    }

    /**
     * Returns null if the vtag does not exist or is not defined for the record.
     */
    public static Record getRecord(RecordId recordId, String vtag, List<QName> fields, Repository repository)
            throws RepositoryException, InterruptedException {

        QName vtagName = new QName(NAMESPACE, vtag);
        Record record = repository.read(recordId);

        long version;
        if (vtag.equals("last")) {
            // we loaded the last version
            if (fields != null) {
                filterFields(record, new HashSet<QName>(fields));
            }
            return record;
        } else if (!record.hasField(vtagName)) {
            return null;
        } else {
            version = (Long)record.getField(vtagName);

            if (version == 0) {
                reduceToNonVersioned(record, fields != null ? new HashSet<QName>(fields) : null,
                        repository.getTypeManager());
            } else {
                record = repository.read(recordId, version, fields);
            }

            return record;
        }
    }

    /**
     * Removes any versioned information from the supplied record object.
     */
    public static void reduceToNonVersioned(Record record, Set<QName> fields, TypeManager typeManager)
            throws RepositoryException, InterruptedException {

        if (record.getVersion() == null) {
            // The record has no versions so there should be no versioned fields in it
            return;
        }

        Iterator<Map.Entry<QName, Object>> fieldsIt = record.getFields().entrySet().iterator();
        while (fieldsIt.hasNext()) {
            Map.Entry<QName, Object> entry = fieldsIt.next();
            if (fields != null && !fields.contains(entry.getKey())) {
                fieldsIt.remove();
            } else if (typeManager.getFieldTypeByName(entry.getKey()).getScope() != Scope.NON_VERSIONED) {
                fieldsIt.remove();
            }
        }

        // Remove versioned record type info
        record.setRecordType(Scope.VERSIONED, (QName)null, null);
        record.setRecordType(Scope.VERSIONED_MUTABLE, (QName)null, null);
    }

    private static void filterFields(Record record, Set<QName> fields) {
        Iterator<Map.Entry<QName, Object>> fieldsIt = record.getFields().entrySet().iterator();
        while (fieldsIt.hasNext()) {
            Map.Entry<QName, Object> entry = fieldsIt.next();
            if (!fields.contains(entry.getKey())) {
                fieldsIt.remove();
            }
        }
    }
}
