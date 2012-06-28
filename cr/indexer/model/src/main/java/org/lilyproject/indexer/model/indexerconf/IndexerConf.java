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
package org.lilyproject.indexer.model.indexerconf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.repo.SystemFields;

// TODO for safety should consider making some of the returned lists immutable

/**
 * The configuration for the indexer, describes how record types should be mapped
 * onto index documents.
 *
 * <p>Fields and vtags are identified by ID in this object model.
 *
 * <p>The IndexerConf is constructed by the {@link IndexerConfBuilder}. Some essential
 * validation is done by that builder which would not be done in case of direct
 * construction.
 */
public class IndexerConf {
    private IndexRecordFilter recordFilter;
    private IndexFields indexFields;
    private Set<SchemaId> repoFieldDependencies = new HashSet<SchemaId>();
    private List<IndexField> derefIndexFields = null;
    private List<DynamicIndexField> dynamicFields = new ArrayList<DynamicIndexField>();
    private Map<SchemaId, List<IndexField>> derefIndexFieldsBySchemaId = null;
    private Set<SchemaId> vtags = new HashSet<SchemaId>();
    private Formatters formatters = new Formatters();
    private SystemFields systemFields;

    protected void setRecordFilter(IndexRecordFilter recordFilter) {
        this.recordFilter = recordFilter;
        for (IndexCase indexCase : recordFilter.getAllIndexCases()) {
            vtags.addAll(indexCase.getVersionTags());
        }
    }

    /**
     * @return null if there is no matching IndexCase
     */
    public IndexCase getIndexCase(Record record) {
        return recordFilter.getIndexCase(record);
    }

    public IndexFields getIndexFields() {
        return indexFields;
    }

    public IndexRecordFilter getRecordFilter() {
        return recordFilter;
    }

    public void setIndexFields(IndexFields indexFields) {
        this.indexFields = indexFields;

        // update information about deref index fields
        derefIndexFields.clear();
        repoFieldDependencies.clear();

        indexFields.visitAll(new Predicate<MappingNode>() {
            @Override
            public boolean apply(MappingNode node) {
                if (node instanceof IndexField) {
                    IndexField indexField = (IndexField)node;
                    SchemaId fieldDep = indexField.getValue().getFieldDependency();
                    if (fieldDep != null)
                        repoFieldDependencies.add(fieldDep);

                    if (indexField.getValue() instanceof DerefValue) {
                        FieldType lastRealField = ((DerefValue)indexField.getValue()).getLastRealField();
                        if (lastRealField != null && !systemFields.isSystemField(lastRealField.getId())) {
                            derefIndexFields.add(indexField);

                            SchemaId fieldId = lastRealField.getId();
                            List<IndexField> fields = derefIndexFieldsBySchemaId.get(fieldId);
                            if (fields == null) {
                                fields = new ArrayList<IndexField>();
                                derefIndexFieldsBySchemaId.put(fieldId, fields);
                            }
                            fields.add(indexField);
                        }
                    }
                }
                return true;
            }
        });
    }

    protected void addDynamicIndexField(DynamicIndexField field) {
        dynamicFields.add(field);
    }

    public List<DynamicIndexField> getDynamicFields() {
        return dynamicFields;
    }

    /**
     * Checks if the supplied field type is used by one of the indexField's.
     */
    public boolean isIndexFieldDependency(FieldType fieldType) {
        boolean staticFieldMatch = repoFieldDependencies.contains(fieldType.getId());

        if (staticFieldMatch) {
            return true;
        }

        // If there are lots of dynamic index fields, or lots of fields which are not indexed at all (thus
        // leading to lots of invocations of this method), than maybe we need to review this for performance.
        for (DynamicIndexField field : dynamicFields) {
            if (field.matches(fieldType).match) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns all IndexField's which have a DerefValue.
     */
    public List<IndexField> getDerefIndexFields() {
        return derefIndexFields;
    }

    /**
     * Returns all IndexFields which have a DerefValue pointing to the given field id, or null if there are none.
     */
    public List<IndexField> getDerefIndexFields(SchemaId fieldId) {
        List<IndexField> result = derefIndexFieldsBySchemaId.get(fieldId);
        return result == null ? Collections.<IndexField>emptyList() : result;
    }

    /**
     * Returns the set of all known vtags, thus all the vtags that are relevant to indexing.
     */
    public Set<SchemaId> getVtags() {
        return vtags;
    }

    public Formatters getFormatters() {
        return formatters;
    }

    public SystemFields getSystemFields() {
        return systemFields;
    }

    public void setSystemFields(SystemFields systemFields) {
        this.systemFields = systemFields;
    }

}
