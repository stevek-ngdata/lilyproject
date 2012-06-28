package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RepositoryException;
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
        // If something changed which may affect the outcome of the match
        // condition,
        // we must reindex (immediately return true)

        if (recordMatcher.dependsOnRecordType()) {
            if (vtRecord.getRecordEvent().getRecordTypeChanged()) {
                return true;
            }
        }

        for (QName fieldTypeName: recordMatcher.getFieldDependencies()) {
            // FIXME: updatedFieldsByScope contains FieldType, not QName
            if (vtRecord.getUpdatedFieldsByScope().get(scope).contains(fieldTypeName)) {
                return true;
            }
        }

        if (recordMatcher.matches(vtRecord.getRecord())) {
            return childrenAffectedByUpdate(vtRecord, scope);
        } else {
            return false;
        }
    }

}
