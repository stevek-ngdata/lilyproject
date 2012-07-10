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


import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public class MatchNode extends ContainerMappingNode {

    private final RecordMatcher recordMatcher;

    public MatchNode(RecordMatcher recordMatcher) {
        this.recordMatcher = recordMatcher;
    }

    public RecordMatcher getRecordMatcher() {
        return recordMatcher;
    }

    /**
     *
     *
     * If the conditions match, we need to evaluate all children
     *
     */
    @Override
    public boolean isIndexAffectedByUpdate(VTaggedRecord vtRecord, Scope scope) throws InterruptedException,
            RepositoryException {
        // If the matcher uses the record type and the record type changed, we must reindex.
        if (recordMatcher.dependsOnRecordType()) {
            if (vtRecord.getRecordEvent().getRecordTypeChanged()) {
                return true;
            }
        }

        // If the matcher uses fields and any of the fields were changed, we must reindex.
        for (FieldType ft: vtRecord.getUpdatedFieldsByScope().get(scope)) {
            if (recordMatcher.getFieldDependencies().contains(ft.getName())) {
                return true;
            }
        }

        if (recordMatcher.matches(vtRecord.getRecord())) {
            return childrenAffectedByUpdate(vtRecord, scope);
        } else {
            return false;
        }
    }

    public void collectIndexUpdate(IndexUpdateBuilder indexUpdateBuilder) throws InterruptedException, RepositoryException  {
        for (SchemaId fieldId: recordMatcher.getFieldDependencyIds()) {
            indexUpdateBuilder.addDependency(fieldId);
        }
        if (recordMatcher.matches(indexUpdateBuilder.getRecordContext().record)) {
            super.collectIndexUpdate(indexUpdateBuilder);
        }
    }

}
