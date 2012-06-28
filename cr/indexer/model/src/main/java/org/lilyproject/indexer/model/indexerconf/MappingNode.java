package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import com.google.common.base.Predicate;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public interface MappingNode {

    /**
     * Returns true if the index can be affected by the given vtRecord
     */
    public abstract boolean isIndexAffectedByUpdate(VTaggedRecord vtRecord, Scope scope) throws InterruptedException,
            RepositoryException;

    /**
     * Collect only the indexfields which need to go into the index.
     */
    public abstract void collectIndexFields(List<IndexField> indexFields, Record record, long version, SchemaId vtag);

    /**
     * Evaluate the predicate for this node. If predicate.apply returns true, descend into children
     */
    public abstract void visitAll(Predicate<MappingNode> predicate);

}
