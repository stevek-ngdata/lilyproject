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

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.repo.SystemFields;

import java.util.*;

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
    private List<IndexCase> indexCases = new ArrayList<IndexCase>();
    private List<IndexField> indexFields = new ArrayList<IndexField>();
    private Set<SchemaId> repoFieldDependencies = new HashSet<SchemaId>();
    private List<IndexField> derefIndexFields = new ArrayList<IndexField>();
    private List<DynamicIndexField> dynamicFields = new ArrayList<DynamicIndexField>();
    private Map<SchemaId, List<IndexField>> derefIndexFieldsByField = new HashMap<SchemaId, List<IndexField>>();
    private Set<SchemaId> vtags = new HashSet<SchemaId>();
    private Formatters formatters = new Formatters();
    private SystemFields systemFields;

    protected void addIndexCase(IndexCase indexCase) {
        indexCases.add(indexCase);
        vtags.addAll(indexCase.getVersionTags());
    }

    /**
     * @return null if there is no matching IndexCase
     */
    public IndexCase getIndexCase(QName recordTypeName, Map<String, String> varProps) {
        for (IndexCase indexCase : indexCases) {
            if (indexCase.match(recordTypeName, varProps)) {
                return indexCase;
            }
        }

        return null;
    }

    public List<IndexField> getIndexFields() {
        return indexFields;
    }

    protected void addIndexField(IndexField indexField) {
        indexFields.add(indexField);

        SchemaId fieldDep = indexField.getValue().getFieldDependency();
        if (fieldDep != null)
            repoFieldDependencies.add(fieldDep);

        if (indexField.getValue() instanceof DerefValue) {
            FieldType lastRealField = ((DerefValue)indexField.getValue()).getLastRealField();
            if (lastRealField != null && !systemFields.isSystemField(lastRealField.getId())) {
                derefIndexFields.add(indexField);

                SchemaId fieldId = lastRealField.getId();
                List<IndexField> fields = derefIndexFieldsByField.get(fieldId);
                if (fields == null) {
                    fields = new ArrayList<IndexField>();
                    derefIndexFieldsByField.put(fieldId, fields);
                }
                fields.add(indexField);
            }
        }
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
        List<IndexField> result = derefIndexFieldsByField.get(fieldId);
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
