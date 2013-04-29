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
import org.lilyproject.tenant.master.TenantMasterHook;
import org.lilyproject.tenant.model.impl.TenantTableUtil;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * A TenantMasterHook responsible for performing core repository actions when a tenant is added or removed.
 */
public class RepositoryTenantMasterHook implements TenantMasterHook {
    private HBaseTableFactory tableFactory;
    private PluginRegistry pluginRegistry;
    private Configuration hbaseConf;
    private final Log log = LogFactory.getLog(getClass());

    public RepositoryTenantMasterHook(HBaseTableFactory tableFactory, Configuration hbaseConf) {
        this.tableFactory = tableFactory;
        this.hbaseConf = hbaseConf;
    }

    public RepositoryTenantMasterHook(HBaseTableFactory tableFactory, Configuration hbaseConf,
            PluginRegistry pluginRegistry) {
        this.tableFactory = tableFactory;
        this.hbaseConf = hbaseConf;
        this.pluginRegistry = pluginRegistry;
    }

    @PostConstruct
    public void postConstruct() {
        pluginRegistry.addPlugin(TenantMasterHook.class, getClass().getSimpleName(), this);
    }

    @PreDestroy
    public void preDestroy() {
        pluginRegistry.removePlugin(TenantMasterHook.class, getClass().getSimpleName(), this);
    }

    @Override
    public void postCreate(String tenantName) throws Exception {
        log.info("Performing tenant post-creation actions.");

        String hbaseTableName = TenantTableUtil.getHBaseTableName(tenantName, LilyHBaseSchema.Table.RECORD.name);
        LilyHBaseSchema.getRecordTable(tableFactory, hbaseTableName, false);
    }

    @Override
    public void preDelete(String tenantName) throws Exception {
        log.info("Performing tenant pre-delete actions.");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
        try {
            String recordTableName = TenantTableUtil.getHBaseTableName(tenantName, "record");
            log.info("Disabling and deleting table " + recordTableName);
            hbaseAdmin.disableTable(recordTableName);
            hbaseAdmin.deleteTable(recordTableName);
        } finally {
            hbaseAdmin.close();
        }
    }
}
