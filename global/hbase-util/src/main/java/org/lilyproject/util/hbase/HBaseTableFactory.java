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
package org.lilyproject.util.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Provides configurability of tables (e.g. the initial splits) and central logic to create tables.
 */
public interface HBaseTableFactory {
    /**
     * Gets the HBase table, creating it if necessary, handling concurrent creates.
     *
     * <p>Returned table instance if a {@link LocalHTable}, so threadsafe.
     */
    HTableInterface getTable(HTableDescriptor tableDescriptor) throws IOException;

    /**
     *
     * @param splitKeys allows the application, rather than the user, to define initial table splits.
     */
    HTableInterface getTable(HTableDescriptor tableDescriptor, byte[][] splitKeys) throws IOException;

    /**
     * @param create if false, table will not be automatically created, TableNotFoundException will be thrown
     *               instead.
     */
    HTableInterface getTable(HTableDescriptor tableDescriptor, boolean create) throws IOException;

    /**
     * @return never null, a default TableConfig is returned if the user did not specify anything.
     */
    TableConfig getTableConfig(byte[] tableName);
    
    void configure(HTableDescriptor tableDescriptor);

    /**
     *
     * @return the split keys for the table, possibly null.
     */
    byte[][] getSplitKeys(byte[] tableName);

}
