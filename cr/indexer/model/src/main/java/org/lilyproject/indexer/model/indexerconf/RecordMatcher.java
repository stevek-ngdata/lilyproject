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
package org.lilyproject.indexer.model.indexerconf;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.repo.RecordUtil;


/**
 * Matches records based on certain conditions, i.e. a predicate on record objects.
 */
public class RecordMatcher {
    public enum FieldComparator {EQUAL, NOT_EQUAL};

    private WildcardPattern recordTypeNamespace;
    private WildcardPattern recordTypeName;

    private QName instanceOfType;

    private FieldType fieldType;
    private FieldComparator fieldComparator;
    private Object fieldValue;

    private TypeManager typeManager;

    /**
     * The variant properties the record should have. Evaluation rules: a key named
     * "*" (star symbol) is a wildcard meaning that any variant dimensions not specified
     * are accepted. Otherwise the variant dimension count should match exactly. The other
     * keys in the map are required variant dimensions. If their value is not null, the
     * values should match.
     */
    private final Map<String, String> variantPropsPattern;

    public RecordMatcher(WildcardPattern recordTypeNamespace, WildcardPattern recordTypeName, QName instanceOfType,
            FieldType fieldType, FieldComparator fieldComparator, Object fieldValue,
            Map<String, String> variantPropsPattern, TypeManager typeManager) {
        this.recordTypeNamespace = recordTypeNamespace;
        this.recordTypeName = recordTypeName;
        this.instanceOfType = instanceOfType;
        this.fieldType = fieldType;
        this.fieldComparator = fieldComparator != null ? fieldComparator : FieldComparator.EQUAL;
        this.fieldValue = fieldValue;
        this.variantPropsPattern = variantPropsPattern;
        this.typeManager = typeManager;
    }

    public boolean matches(Record record) {
        QName recordTypeName = record.getRecordTypeName();
        Map<String, String> varProps = record.getId().getVariantProperties();

        // About "recordTypeName == null": normally record type name cannot be null, but it can
        // be in the case of IndexAwareMQFeeder
        if (this.recordTypeNamespace != null &&
                (recordTypeName == null || !this.recordTypeNamespace.lightMatch(recordTypeName.getNamespace()))) {
            return false;
        }

        if (this.recordTypeName != null
                && (recordTypeName == null || !this.recordTypeName.lightMatch(recordTypeName.getName()))) {
            return false;
        }

        try {
            if (this.instanceOfType != null
                    && (recordTypeName == null || !RecordUtil.instanceOf(record, instanceOfType, typeManager))) {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        if (variantPropsPattern != null) {
            if (variantPropsPattern.size() != varProps.size() && !variantPropsPattern.containsKey("*")) {
                return false;
            }

            for (Map.Entry<String, String> entry : variantPropsPattern.entrySet()) {
                if (entry.getKey().equals("*"))
                    continue;

                String dimVal = varProps.get(entry.getKey());
                if (dimVal == null) {
                    // this record does not have a required variant property
                    return false;
                }

                if (entry.getValue() != null && !entry.getValue().equals(dimVal)) {
                    // the variant property does not have the required value
                    return false;
                }
            }
        }

        if (fieldType != null) {
            if (fieldComparator == FieldComparator.EQUAL) {
                return record.hasField(fieldType.getName()) && fieldValue.equals(record.getField(fieldType.getName()));
            } else if (fieldComparator == FieldComparator.NOT_EQUAL) {
                // not-equal should evaluate to true if field value is null
                return !record.hasField(fieldType.getName()) || !fieldValue.equals(record.getField(fieldType.getName()));
            } else {
                throw new RuntimeException("Unexpected comparison operator: " + fieldComparator);
            }
        }

        return true;
    }

    public Set<QName> getFieldDependencies() {
        return fieldType != null ? Collections.singleton(fieldType.getName()) : Collections.<QName>emptySet();
    }

    public Set<SchemaId> getFieldDependencyIds() {
        return fieldType != null ? Collections.singleton(fieldType.getId()) : Collections.<SchemaId>emptySet();
    }

    public boolean dependsOnRecordType() {
        return recordTypeName != null || recordTypeNamespace != null;
    }
}
