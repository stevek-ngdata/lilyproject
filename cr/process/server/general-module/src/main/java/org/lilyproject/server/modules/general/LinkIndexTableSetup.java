package org.lilyproject.server.modules.general;

import org.apache.hadoop.conf.Configuration;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.util.hbase.HBaseTableFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class LinkIndexTableSetup {
    private final Configuration hbaseConf;
    private final HBaseTableFactory tableFactory;

    public LinkIndexTableSetup(Configuration hbaseConf, HBaseTableFactory tableFactory) {
        this.hbaseConf = hbaseConf;
        this.tableFactory = tableFactory;
    }

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        IndexManager indexManager = new IndexManager(hbaseConf, tableFactory);
    }
}
