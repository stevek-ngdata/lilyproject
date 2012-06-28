package org.lilyproject.indexer.model.util;

import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;

public class IndexInfo {
    IndexDefinition indexDefinition;
    IndexerConf indexerConf;

    public IndexInfo(IndexDefinition indexDefinition, IndexerConf indexerConf) {
        this.indexDefinition = indexDefinition;
        this.indexerConf = indexerConf;
    }

    public IndexDefinition getIndexDefinition() {
        return indexDefinition;
    }

    public IndexerConf getIndexerConf() {
        return indexerConf;
    }
}
