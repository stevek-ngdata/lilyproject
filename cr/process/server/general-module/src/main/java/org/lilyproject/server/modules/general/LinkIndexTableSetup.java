package org.lilyproject.server.modules.general;

import org.apache.hadoop.conf.Configuration;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.util.hbase.TableConfig;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class LinkIndexTableSetup {
    private final Configuration hbaseConf;
    private final TableConfig tableConfig;

    public LinkIndexTableSetup(Configuration hbaseConf, TableConfig tableConfig) {
        this.hbaseConf = hbaseConf;
        this.tableConfig = tableConfig;
    }

    @PostConstruct
    public void init() throws IOException {
        IndexManager.createIndexMetaTableIfNotExists(hbaseConf);
        IndexManager indexManager = new IndexManager(hbaseConf);

        // Each index field is prefixed with a meta-byte, which, if the field is not null, which is the case
        // for the link index, is currently always 0.
        byte[][] splitKeys = tableConfig.getSplitKeys(new byte[]{0});
        LinkIndex.createIndexes(indexManager, splitKeys);
    }
}
