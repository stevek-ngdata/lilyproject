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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VTaggedRecord;

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
    private List<DynamicIndexField> dynamicFields = new ArrayList<DynamicIndexField>();
    private Set<SchemaId> vtags = new HashSet<SchemaId>();
    private Formatters formatters = new Formatters();
    private SystemFields systemFields;
    private boolean containsDerefExpression = false;

    protected void setRecordFilter(IndexRecordFilter recordFilter) {
        this.recordFilter = recordFilter;
        for (IndexCase indexCase : recordFilter.getAllIndexCases()) {
            vtags.addAll(indexCase.getVersionTags());
        }
    }

    /**
     * @return null if there is no matching IndexCase
     */
    public IndexCase getIndexCase(String table, Record record) {
        return recordFilter.getIndexCase(table, record);
    }

    public IndexFields getIndexFields() {
        return indexFields;
    }

    public IndexRecordFilter getRecordFilter() {
        return recordFilter;
    }

    public void setIndexFields(IndexFields indexFields) {
        this.indexFields = indexFields;

        indexFields.visitAll(new Predicate<MappingNode>() {
            @Override
            public boolean apply(MappingNode node) {
                if (node instanceof IndexField) {
                    IndexField indexField = (IndexField) node;
                    if (indexField.getValue() instanceof DerefValue) {
                        containsDerefExpression = true;
                    }
                } else if (node instanceof ForEachNode) {
                    containsDerefExpression = true;
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

    public boolean containsDerefExpressions() {
        return containsDerefExpression;
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

    public boolean changesAffectIndex(VTaggedRecord vtRecord, Scope scope)
            throws InterruptedException, RepositoryException {
        Set<FieldType> changedFields = vtRecord.getRecordEventHelper().getUpdatedFieldsByScope().get(scope);

        // Check <fields>
        boolean affects = indexFields.isIndexAffectedByUpdate(vtRecord, scope);
        if (affects) {
            return true;
        }

        // Check dynamic fields
        for (FieldType fieldType : changedFields) {
            // If there are lots of dynamic index fields, or lots of fields which are not indexed at all (thus
            // leading to lots of invocations of this method), than maybe we need to review this for performance.
            for (DynamicIndexField field : dynamicFields) {
                if (field.matches(fieldType).match) {
                    return true;
                }
            }
        }

        return false;
    }

}
