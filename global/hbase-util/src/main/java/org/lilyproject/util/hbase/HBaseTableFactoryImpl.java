/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.util.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableFactoryImpl implements HBaseTableFactory {
    private Log log = LogFactory.getLog(getClass());
    private final Configuration configuration;
    private final List<TableConfigEntry> tableConfigs;
    private final ColumnFamilyConfig defaultCfConfig;

    public HBaseTableFactoryImpl(Configuration configuration) {
        this(configuration, null, new ColumnFamilyConfig());
    }

    public HBaseTableFactoryImpl(Configuration configuration, List<TableConfigEntry> tableConfigs,
            ColumnFamilyConfig defaultCfConfig) {
        this.configuration = configuration;
        this.tableConfigs = tableConfigs == null ? Collections.<TableConfigEntry>emptyList() : tableConfigs;
        this.defaultCfConfig = defaultCfConfig;
    }

    @Override
    public HTableInterface getTable(HTableDescriptor tableDescriptor) throws IOException, InterruptedException {
        return getTable(tableDescriptor, true);
    }

    @Override
    public HTableInterface getTable(HTableDescriptor tableDescriptor, byte[][] splitKeys) throws IOException, InterruptedException {
        return getTable(tableDescriptor, splitKeys, true);
    }

    @Override
    public HTableInterface getTable(HTableDescriptor tableDescriptor, boolean create) throws IOException, InterruptedException {
        return getTable(tableDescriptor, getSplitKeys(tableDescriptor.getName()), create);
    }

    private HTableInterface getTable(HTableDescriptor tableDescriptor, byte[][] splitKeys, boolean create) throws IOException, InterruptedException {
        HBaseAdmin admin = new HBaseAdmin(configuration);

        try {
            try {
                admin.getTableDescriptor(tableDescriptor.getName());
            } catch (TableNotFoundException e) {
                if (!create) {
                    throw e;
                }
    
                try {
                    // Make a deep copy, we don't want to touch the original
                    tableDescriptor = new HTableDescriptor(tableDescriptor);
                    configure(tableDescriptor);
    
                    int regionCount = splitKeys == null ? 1 : splitKeys.length + 1;
                    log.info("Creating '" + tableDescriptor.getNameAsString() + "' table using "
                            + regionCount + " initial region" + (regionCount > 1 ? "s." : "."));
                    admin.createTable(tableDescriptor, splitKeys);
                } catch (TableExistsException e2) {
                    // Table is meanwhile created by another process
                    log.info("Table already existed: '" + tableDescriptor.getNameAsString() + "'.");
    
                }
            }
      
            //In all cases we need to wait until the table is available
            // https://issues.apache.org/jira/browse/HBASE-6576
            long startWait = System.currentTimeMillis();
            long timeoutMillis = 2 * 60 * 1000L; // two minutes
            while (!admin.isTableAvailable(tableDescriptor.getName())) {
                if (System.currentTimeMillis() - startWait > timeoutMillis) {
                    throw new IOException("Timed out waiting for table " + Bytes.toStringBinary(tableDescriptor.getName()));
                };
                Thread.sleep(200);
            }

            // TODO we could check if the existing table matches the given table descriptor
            return new LocalHTable(configuration, tableDescriptor.getName());
        } finally {
            admin.close();
        }
    }

    @Override
    public void configure(HTableDescriptor tableDescriptor) {
        TableConfig tableConfig = getTableConfig(tableDescriptor.getName());

        // max file size
        Long maxFileSize = tableConfig.getMaxFileSize();
        if (maxFileSize != null)
            tableDescriptor.setMaxFileSize(maxFileSize);

        // memstore flush size
        Long memStoreFlushSize = tableConfig.getMemStoreFlushSize();
        if (memStoreFlushSize != null)
            tableDescriptor.setMemStoreFlushSize(memStoreFlushSize);

        for (HColumnDescriptor column : tableDescriptor.getColumnFamilies()) {
            ColumnFamilyConfig cfConf = tableConfig.getColumnFamilyConfig(column.getNameAsString());

            // compression
            Compression.Algorithm compression = cfConf.getCompression() != null ?
                    cfConf.getCompression() : defaultCfConfig.getCompression();
            if (compression != null) {
                column.setCompressionType(compression);
            }

            // block size
            Integer blockSize = cfConf.getBlockSize() != null ?
                    cfConf.getBlockSize() : defaultCfConfig.getBlockSize();
            if (blockSize != null) {
                column.setBlocksize(blockSize);
            }

            // bloom filter
            StoreFile.BloomType bloomFilter = cfConf.getBoomFilter() != null ?
                    cfConf.getBoomFilter() : defaultCfConfig.getBoomFilter();
            if (bloomFilter != null) {
                column.setBloomFilterType(bloomFilter);
            }
        }
    }

    @Override
    public TableConfig getTableConfig(byte[] tableName) {
        String tableNameString = Bytes.toString(tableName);
        for (TableConfigEntry entry : tableConfigs) {
            if (entry.matches(tableNameString)) {
                log.debug("Table configuration settings: table \"" + tableNameString + "\" matched regex \""
                        + entry.getTableNamePattern().pattern() + "\"");
                return entry.getTableConfig();
            }
        }
        log.debug("Table configuration settings: table \"" + tableNameString + "\" has no matching settings.");
        return new TableConfig();
    }

    @Override
    public byte[][] getSplitKeys(byte[] tableName) {
        return getTableConfig(tableName).getSplitKeys();
    }
}
