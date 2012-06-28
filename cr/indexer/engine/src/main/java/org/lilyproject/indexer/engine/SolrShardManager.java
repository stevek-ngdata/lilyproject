package org.lilyproject.indexer.engine;

import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.RecordId;

public interface SolrShardManager {
    SolrClient getSolrClient(RecordId recordId) throws ShardSelectorException;
}
