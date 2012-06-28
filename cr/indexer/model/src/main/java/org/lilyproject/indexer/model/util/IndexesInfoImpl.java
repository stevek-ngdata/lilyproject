package org.lilyproject.indexer.model.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.indexer.model.api.IndexDefinition;
import org.lilyproject.indexer.model.api.IndexerModel;
import org.lilyproject.indexer.model.api.IndexerModelEvent;
import org.lilyproject.indexer.model.api.IndexerModelListener;
import org.lilyproject.indexer.model.indexerconf.IndexRecordFilter;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Repository;

import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 * See {@link IndexesInfo}.
 */
public class IndexesInfoImpl implements IndexesInfo {
    private IndexerModel indexerModel;
    private Repository repository;

    private Map<String, IndexInfo> indexInfos;
    private Set<QName> recordFilterFieldDependencies;
    private boolean recordFilterDependsOnRecordType;

    private Listener listener = new Listener();
    private Log log = LogFactory.getLog(getClass());
    private ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1), new ThreadPoolExecutor.DiscardPolicy());

    public IndexesInfoImpl(IndexerModel indexerModel, Repository repository) {
        this.indexerModel = indexerModel;
        this.repository = repository;

        indexerModel.registerListener(listener);
        refresh();
    }

    @PreDestroy
    public void stop() {
        executor.shutdown();
    }

    private synchronized void refresh() {
        Map<String, IndexInfo> newIndexInfos = new HashMap<String, IndexInfo>();

        Collection<IndexDefinition> indexDefs = indexerModel.getIndexes();
        for (IndexDefinition indexDef : indexDefs) {
            byte[] indexerConfXml = indexDef.getConfiguration();
            IndexerConf indexerConf = null;
            try {
                indexerConf = IndexerConfBuilder.build(new ByteArrayInputStream(indexerConfXml), repository);
            } catch (Throwable t) {
                log.error("Error parsing indexer conf", t);
            }

            // If parsing failed, we exclude the index
            if (indexerConf != null) {
                newIndexInfos.put(indexDef.getName(), new IndexInfo(indexDef, indexerConf));
            }
        }

        // Pre-calculate some cross-index information
        Set<QName> recordFilterFieldDependencies = new HashSet<QName>();
        boolean recordFilterDependsOnRecordType = false;
        for (IndexInfo indexInfo : newIndexInfos.values()) {
            IndexRecordFilter recordFilter = indexInfo.getIndexerConf().getRecordFilter();
            recordFilterFieldDependencies.addAll(recordFilter.getFieldDependencies());
            if (!recordFilterDependsOnRecordType) {
                recordFilterDependsOnRecordType = recordFilter.dependsOnRecordType();
            }
        }

        this.indexInfos = newIndexInfos;
        this.recordFilterFieldDependencies = recordFilterFieldDependencies;
        this.recordFilterDependsOnRecordType = recordFilterDependsOnRecordType;
    }

    @Override
    public Collection<IndexInfo> getIndexInfos() {
        return indexInfos.values();
    }

    public Set<QName> getRecordFilterFieldDependencies() {
        return recordFilterFieldDependencies;
    }

    public boolean getRecordFilterDependsOnRecordType() {
        return recordFilterDependsOnRecordType;
    }

    private class Listener implements IndexerModelListener {
        @Override
        public void process(IndexerModelEvent event) {
            // The refresh is called asynchronously, in order not to block the delivery of
            // other events (cfr. single ZK event dispatch thread).

            // If there is still an outstanding refresh task waiting in the queue, we don't
            // need to add another one. The configuration of the ExecutorService takes care
            // of this (queue bounded to 1 item + discard policy).

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    refresh();
                }
            });
        }
    }
}
