package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
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

    public void collectIndexFields(List<IndexField> indexFields, Record record, long version, SchemaId vtag) {
        if (recordMatcher.matches(record)) {
            super.collectIndexFields(indexFields, record, version, vtag);
        }
    }

}
