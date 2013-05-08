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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.util.repo.RepoAndTableUtil;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

public class TableManagerImpl implements TableManager {

    private String repositoryName;
    private Configuration configuration;
    private HBaseTableFactory tableFactory;

    public TableManagerImpl(String repositoryName, Configuration configuration, HBaseTableFactory tableFactory) {
        this.repositoryName = repositoryName;
        this.configuration = configuration;
        this.tableFactory = tableFactory;
    }

    @Override
    public RepositoryTable createTable(String tableName) throws InterruptedException, IOException {
        return createTable(new TableCreateDescriptor(tableName));
    }

    @Override
    public RepositoryTable createTable(TableCreateDescriptor descriptor) throws InterruptedException, IOException {
        if (!RepoAndTableUtil.isValidTableName(descriptor.getName())) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid table name. "
                    + RepoAndTableUtil.VALID_NAME_EXPLANATION, descriptor.getName()));
        }
        if (tableExists(descriptor.getName())) {
            throw new IllegalArgumentException(String.format("Table '%s' already exists", descriptor.getName()));
        }
        String hbaseTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, descriptor.getName());
        LilyHBaseSchema.getRecordTable(tableFactory, hbaseTableName, descriptor.getSplitKeys());
        return new RepositoryTableImpl(repositoryName, descriptor.getName());
    }

    @Override
    public void dropTable(String tableName) throws InterruptedException, IOException {

        if (Table.RECORD.name.equals(tableName)) {
            throw new IllegalArgumentException("Can't delete the default record table");
        }

        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        String hbaseTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, tableName);

        try {
            if (hbaseAdmin.tableExists(hbaseTableName)
                    && LilyHBaseSchema.isRecordTableDescriptor(hbaseAdmin.getTableDescriptor(Bytes.toBytes(hbaseTableName)))) {
                hbaseAdmin.disableTable(hbaseTableName);
                hbaseAdmin.deleteTable(hbaseTableName);
            } else {
                throw new IllegalArgumentException(
                        String.format("Table '%s' is not a valid record table (HBase table name: '%s')",
                                tableName, hbaseTableName));
            }
        } finally {
            hbaseAdmin.close();
        }

    }

    @Override
    public List<RepositoryTable> getTables() throws InterruptedException, IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        List<RepositoryTable> recordTables = Lists.newArrayList();
        try {
            for (HTableDescriptor tableDescriptor : hbaseAdmin.listTables()) {
                if (LilyHBaseSchema.isRecordTableDescriptor(tableDescriptor)
                        && RepoAndTableUtil.belongsToRepository(tableDescriptor.getNameAsString(), repositoryName)) {
                    String name = RepoAndTableUtil.extractLilyTableName(repositoryName, tableDescriptor.getNameAsString());
                    recordTables.add(new RepositoryTableImpl(repositoryName, name));
                }
            }
        } finally {
            hbaseAdmin.close();
        }
        return recordTables;
    }

    @Override
    public boolean tableExists(String tableName) throws InterruptedException, IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        try {
            return hbaseAdmin.tableExists(getHBaseTableName(tableName));
        } finally {
            hbaseAdmin.close();
        }
    }

    private String getHBaseTableName(String tableName) {
        return new RepoTableKey(repositoryName, tableName).toHBaseTableName();
    }

}
