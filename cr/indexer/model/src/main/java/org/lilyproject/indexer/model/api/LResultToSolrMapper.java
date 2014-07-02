package org.lilyproject.indexer.model.api;

import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.lilyproject.repository.api.LRepository;

public interface LResultToSolrMapper extends ResultToSolrMapper {
    // These represent the names of connection params
    static final String ZOOKEEPER_KEY = "lily.zk"; // required
    static final String REPO_KEY = "lily.repository"; // defaults to 'default'
    static final String TABLE_KEY = "lily.table"; // defaults to 'record'
    static final String ENABLE_DEREFMAP_KEY = "lily.enable-derefmap"; // defaults to 'true'

    LRepository getRepository();
}
