package org.lilyproject.indexer.model.api;

import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.lilyproject.repository.api.LRepository;

public interface LResultToSolrMapper extends ResultToSolrMapper {
    static final String ZOOKEEPER_KEY = "zookeeper";
    static final String REPO_KEY = "repository";
    static final String TABLE_KEY = "table";
    static final String INDEXERCONF_KEY = "indexerConf";
    static final String INDEX_KEY = "name";

    LRepository getRepository();
}
