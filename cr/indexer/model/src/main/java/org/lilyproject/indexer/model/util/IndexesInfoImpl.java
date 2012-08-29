/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.model.util;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

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

/**
 * See {@link IndexesInfo}.
 */
public class IndexesInfoImpl implements IndexesInfo {
    private final IndexerModel indexerModel;
    private final Repository repository;

    private Map<String, IndexInfo> indexInfos;
    private Set<QName> recordFilterFieldDependencies;
    private boolean recordFilterDependsOnRecordType;

    private final Listener listener = new Listener();
    private final Log log = LogFactory.getLog(getClass());
    private final ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(1), new ThreadPoolExecutor.DiscardPolicy());

    public IndexesInfoImpl(IndexerModel indexerModel, Repository repository) {
        this.indexerModel = indexerModel;
        this.repository = repository;

        indexerModel.registerListener(listener);

        // do refresh in a separate thread since building indexes depends on the repository being up and running
        // which might not be the case. Doing a refresh in another thread will give the repository a chance to
        // start up. We do this by calling the listener
        this.listener.process(null);

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

    @Override
    public Set<QName> getRecordFilterFieldDependencies() {
        return recordFilterFieldDependencies;
    }

    @Override
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
