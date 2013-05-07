/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.master.RepositoryMasterHook;
import org.lilyproject.util.repo.RepoAndTableUtil;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * A RepositoryMasterHook responsible for performing core repository actions when a repository is added or removed.
 */
public class CoreRepositoryMasterHook implements RepositoryMasterHook {
    private HBaseTableFactory tableFactory;
    private PluginRegistry pluginRegistry;
    private Configuration hbaseConf;
    private final Log log = LogFactory.getLog(getClass());

    public CoreRepositoryMasterHook(HBaseTableFactory tableFactory, Configuration hbaseConf) {
        this.tableFactory = tableFactory;
        this.hbaseConf = hbaseConf;
    }

    public CoreRepositoryMasterHook(HBaseTableFactory tableFactory, Configuration hbaseConf,
            PluginRegistry pluginRegistry) {
        this.tableFactory = tableFactory;
        this.hbaseConf = hbaseConf;
        this.pluginRegistry = pluginRegistry;
    }

    @PostConstruct
    public void postConstruct() {
        pluginRegistry.addPlugin(RepositoryMasterHook.class, getClass().getSimpleName(), this);
    }

    @PreDestroy
    public void preDestroy() {
        pluginRegistry.removePlugin(RepositoryMasterHook.class, getClass().getSimpleName(), this);
    }

    @Override
    public void postCreate(String repositoryName) throws Exception {
        log.info("Performing repository post-creation actions for repository " + repositoryName);

        String hbaseTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, LilyHBaseSchema.Table.RECORD.name);
        LilyHBaseSchema.getRecordTable(tableFactory, hbaseTableName, false);
    }

    @Override
    public void preDelete(String repositoryName) throws Exception {
        log.info("Performing repository pre-delete actions for repository " + repositoryName);

        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
        try {
            String recordTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, "record");
            log.info("Disabling and deleting table " + recordTableName);
            hbaseAdmin.disableTable(recordTableName);
            hbaseAdmin.deleteTable(recordTableName);
        } finally {
            hbaseAdmin.close();
        }
    }
}
