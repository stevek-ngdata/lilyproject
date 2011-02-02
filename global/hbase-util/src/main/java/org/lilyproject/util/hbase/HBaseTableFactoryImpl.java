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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.util.ByteArrayKey;

public class HBaseTableFactoryImpl implements HBaseTableFactory {    
    private Log log = LogFactory.getLog(getClass());
    private final Configuration configuration;
    private final Map<ByteArrayKey, TableConfig> tableConfigs;

    public HBaseTableFactoryImpl(Configuration configuration) {
        this(configuration, null);
    }

    public HBaseTableFactoryImpl(Configuration configuration, Map<ByteArrayKey, TableConfig> tableConfigs) {
        this.configuration = configuration;
        this.tableConfigs = tableConfigs == null ? Collections.<ByteArrayKey, TableConfig>emptyMap() : tableConfigs;
    }

    public HTableInterface getTable(HTableDescriptor tableDescriptor) throws IOException {
        return getTable(tableDescriptor, true);
    }

    public HTableInterface getTable(HTableDescriptor tableDescriptor, boolean create) throws IOException {
        return getTable(tableDescriptor, getSplitKeys(tableDescriptor.getName()), create);
    }

    private HTableInterface getTable(HTableDescriptor tableDescriptor, byte[][] splitKeys, boolean create) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);

        try {
            admin.getTableDescriptor(tableDescriptor.getName());
        } catch (TableNotFoundException e) {
            if (!create) {
                throw e;
            }

            try {
                int regionCount = splitKeys == null ? 1 : splitKeys.length + 1;
                log.info("Creating '" + tableDescriptor.getNameAsString() + "' table using "
                        + regionCount + " initial region" + (regionCount > 1 ? "s.": "."));
                admin.createTable(tableDescriptor, splitKeys);
            } catch (TableExistsException e2) {
                // Table is meanwhile created by another process
                log.info("Table already existed: '" + tableDescriptor.getNameAsString() + "'.");
            }
        }

        // TODO we could check if the existing table matches the given table descriptor

        return new LocalHTable(configuration, tableDescriptor.getName());
    }

    public TableConfig getTableConfig(byte[] tableName) {
        TableConfig config = tableConfigs.get(new ByteArrayKey(tableName));
        return config != null ? config : new TableConfig();
    }

    public byte[][] getSplitKeys(byte[] tableName) {
        TableConfig config = tableConfigs.get(new ByteArrayKey(tableName));
        byte[][] splitKeys = config != null ? config.getSplitKeys() : null;
        return splitKeys;
    }

}
