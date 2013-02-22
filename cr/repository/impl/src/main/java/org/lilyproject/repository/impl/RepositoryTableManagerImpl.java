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
import org.lilyproject.repository.api.RepositoryTableManager;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

public class RepositoryTableManagerImpl implements RepositoryTableManager {

    private Configuration configuration;
    private HBaseTableFactory tableFactory;

    public RepositoryTableManagerImpl(Configuration configuration, HBaseTableFactory tableFactory) {
        this.configuration = configuration;
        this.tableFactory = tableFactory;
    }

    @Override
    public void createTable(String tableName) throws InterruptedException, IOException {
        createTable(tableName, null);
    }
    
    @Override
    public void createTable(String tableName, byte[][] splitKeys) throws InterruptedException, IOException {
        if (tableExists(tableName)) {
            throw new IllegalArgumentException(String.format("Table '%s' already exists", tableName));
        }
        LilyHBaseSchema.getRecordTable(tableFactory, tableName, splitKeys);
    }

    @Override
    public void dropTable(String tableName) throws InterruptedException, IOException {
        
        if (Table.RECORD.name.equals(tableName)) {
            throw new IllegalArgumentException("Can't delete the default record table");
        }
        
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        
        try {
            if (hbaseAdmin.tableExists(tableName)
                    && LilyHBaseSchema.isRecordTableDescriptor(hbaseAdmin.getTableDescriptor(Bytes.toBytes(tableName)))) {
                hbaseAdmin.disableTable(tableName);
                hbaseAdmin.deleteTable(tableName);
            } else {
                throw new IllegalArgumentException(String.format("Table '%s' is not a valid record table", tableName));
            }
        } finally {
            hbaseAdmin.close();
        }

    }

    @Override
    public List<String> getTableNames() throws InterruptedException, IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        List<String> recordTableNames = Lists.newArrayList();
        try {
            for (HTableDescriptor tableDescriptor : hbaseAdmin.listTables()) {
                if (LilyHBaseSchema.isRecordTableDescriptor(tableDescriptor)) {
                    recordTableNames.add(tableDescriptor.getNameAsString());
                }
            }
        } finally {
            hbaseAdmin.close();
        }
        return recordTableNames;
    }

    @Override
    public boolean tableExists(String tableName) throws InterruptedException, IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        try {
            return hbaseAdmin.tableExists(tableName);
        } finally {
            hbaseAdmin.close();
        }
    }

}
