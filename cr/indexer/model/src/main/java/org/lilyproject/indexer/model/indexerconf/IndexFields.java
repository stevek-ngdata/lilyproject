package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

/**
 * The root of the mapping hierarchy. This corresponds to the 'fields' element in indexerconf.
 */
public class IndexFields extends ContainerMappingNode {

    @Override
    public boolean isIndexAffectedByUpdate(VTaggedRecord record, Scope scope) throws InterruptedException, RepositoryException {
        return childrenAffectedByUpdate(record, scope);
    }

}
