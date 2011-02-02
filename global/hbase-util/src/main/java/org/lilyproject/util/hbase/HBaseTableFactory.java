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

    HTableInterface getTable(HTableDescriptor tableDescriptor, boolean create) throws IOException;

    /**
     * @return never null, a default TableConfig is returned if the user did not specify anything.
     */
    TableConfig getTableConfig(byte[] tableName);

    /**
     *
     * @return the split keys for the table, possibly null.
     */
    byte[][] getSplitKeys(byte[] tableName);

}
