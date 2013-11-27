package org.lilyproject.indexer.hbase.mapper;

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerLifecycleListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;

import java.io.IOException;

public class LilyIndexerLifecycleEventListener implements IndexerLifecycleListener, Configurable{
    private final Log log = LogFactory.getLog(getClass());

    private Configuration hbaseConf;

    @Override
    public Configuration getConf() {
        return hbaseConf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.hbaseConf = conf;
    }

    @Override
    public void onBatchBuild(IndexerDefinition indexerDefinition) {
        // ignore
    }

    @Override
    public void onDelete(IndexerDefinition indexerDefinition) {
        //clean up derefmap
        String indexName = indexerDefinition.getName();
        try {
            DerefMapHbaseImpl.delete(indexName, hbaseConf);
        } catch (IOException e) {
            log.error("Failed to delete DerefMap for index " + indexName, e);
        } catch (org.lilyproject.hbaseindex.IndexNotFoundException e) {
            // ignore, the index was already deleted
        }
    }

    @Override
    public void onSubscribe(IndexerDefinition indexerDefinition) {
        // ignore
    }

    @Override
    public void onUnsubscribe(IndexerDefinition indexerDefinition) {
        //ignore
    }
}
