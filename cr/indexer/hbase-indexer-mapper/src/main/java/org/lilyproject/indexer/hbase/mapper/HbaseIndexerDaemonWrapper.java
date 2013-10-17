package org.lilyproject.indexer.hbase.mapper;

import com.ngdata.hbaseindexer.ConfKeys;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.Main;
import com.yammer.metrics.reporting.GangliaReporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.TimeUnit;

public class HbaseIndexerDaemonWrapper {
    private Configuration hbaseConf;
    private com.ngdata.hbaseindexer.Main daemon;

    private Log log = LogFactory.getLog(HbaseIndexerDaemonWrapper.class);


    public Configuration getHbaseConf() {
        return hbaseConf;
    }

    public void setHbaseConf(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf;
    }

    public void destroy() {
        this.daemon.stopServices();
    }

    public void run () throws Exception {
        Configuration indexerConfiguration = HBaseIndexerConfiguration.addHbaseIndexerResources(this.hbaseConf);
        setupMetrics(indexerConfiguration);
        this.daemon = new Main();
        this.daemon.startServices(indexerConfiguration);
    }

    /* borrowed this code from com.ngdata.hbaseindexer.Main */
    private void setupMetrics(Configuration conf) {
        String gangliaHost = conf.get(ConfKeys.GANGLIA_SERVER);
        if (gangliaHost != null) {
            int gangliaPort = conf.getInt(ConfKeys.GANGLIA_PORT, 8649);
            int interval = conf.getInt(ConfKeys.GANGLIA_INTERVAL, 60);
            log.info("Enabling Ganglia reporting to " + gangliaHost + ":" + gangliaPort);
            GangliaReporter.enable(interval, TimeUnit.SECONDS, gangliaHost, gangliaPort);
        }
    }


}
