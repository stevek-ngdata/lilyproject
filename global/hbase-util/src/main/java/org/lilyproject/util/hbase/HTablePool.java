// Do not add Lily license: this file is copied from the HBase source tree
package org.lilyproject.util.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// Copied from CDH3u2, with as modification that we don't clone the Configuration object,
// in order to avoid setting up additional HBase & ZooKeeper connections (and to avoid
// leaks due to unclosed connections when using resetLilyState)
public class HTablePool {
    private final ConcurrentMap<String, LinkedList<HTableInterface>> tables =
            new ConcurrentHashMap<String, LinkedList<HTableInterface>>();
    private final Configuration config;
    private final int maxSize;
    private final HTableInterfaceFactory tableFactory;

    /**
     * Default Constructor.  Default HBaseConfiguration and no limit on pool size.
     */
    public HTablePool() {
        this(HBaseConfiguration.create(), Integer.MAX_VALUE);
    }

    /**
     * Constructor to set maximum versions and use the specified configuration.
     * @param config configuration
     * @param maxSize maximum number of references to keep for each table
     */
    public HTablePool(final Configuration config, final int maxSize) {
        this(config, maxSize, null);
    }

    public HTablePool(final Configuration config, final int maxSize,
            final HTableInterfaceFactory tableFactory) {
        // Make a new configuration instance so I can safely cleanup when
        // done with the pool.
        // Modified for Lily: don't clone Configuration object
        this.config = config;
        this.maxSize = maxSize;
        this.tableFactory = tableFactory == null? new HTableFactory(): tableFactory;
    }

    /**
     * Get a reference to the specified table from the pool.<p>
     *
     * Create a new one if one is not available.
     * @param tableName table name
     * @return a reference to the specified table
     * @throws RuntimeException if there is a problem instantiating the HTable
     */
    public HTableInterface getTable(String tableName) {
        LinkedList<HTableInterface> queue = tables.get(tableName);
        if(queue == null) {
            queue = new LinkedList<HTableInterface>();
            tables.putIfAbsent(tableName, queue);
            return createHTable(tableName);
        }
        HTableInterface table;
        synchronized(queue) {
            table = queue.poll();
        }
        if(table == null) {
            return createHTable(tableName);
        }
        return table;
    }

    /**
     * Get a reference to the specified table from the pool.<p>
     *
     * Create a new one if one is not available.
     * @param tableName table name
     * @return a reference to the specified table
     * @throws RuntimeException if there is a problem instantiating the HTable
     */
    public HTableInterface getTable(byte [] tableName) {
        return getTable(Bytes.toString(tableName));
    }

    /**
     * Puts the specified HTable back into the pool.<p>
     *
     * If the pool already contains <i>maxSize</i> references to the table,
     * then the table instance gets closed after flushing buffered edits.
     * @param table table
     */
    public void putTable(HTableInterface table) {
        LinkedList<HTableInterface> queue = tables.get(Bytes.toString(table.getTableName()));
        synchronized(queue) {
            if(queue.size() >= maxSize) {
                // release table instance since we're not reusing it
            	try {
            		this.tableFactory.releaseHTableInterface(table);
            	} catch (IOException e) {
            		// TODO Do we need HTablePool ?
            		throw new RuntimeException("IOException, should be handled.", e);
            	}
                return;
            }
            queue.add(table);
        }
    }

    protected HTableInterface createHTable(String tableName) {
        return this.tableFactory.createHTableInterface(config, Bytes.toBytes(tableName));
    }

    /**
     * Closes all the HTable instances , belonging to the given table, in the table pool.
     * <p>
     * Note: this is a 'shutdown' of the given table pool and different from
     * {@link #putTable(HTableInterface)}, that is used to return the table
     * instance to the pool for future re-use.
     *
     * @param tableName
     */
    public void closeTablePool(final String tableName)  {
        Queue<HTableInterface> queue = tables.get(tableName);
        synchronized (queue) {
            HTableInterface table = queue.poll();
            while (table != null) {
            	try {
            		this.tableFactory.releaseHTableInterface(table);
	            } catch (IOException e) {
	        		// TODO Do we need HTablePool ?
	        		throw new RuntimeException("IOException, should be handled.", e);
	        	}
                table = queue.poll();
            }
        }
        HConnectionManager.deleteConnection(this.config, true);
    }

    /**
     * See {@link #closeTablePool(String)}.
     *
     * @param tableName
     */
    public void closeTablePool(final byte[] tableName)  {
        closeTablePool(Bytes.toString(tableName));
    }

    int getCurrentPoolSize(String tableName) {
        Queue<HTableInterface> queue = tables.get(tableName);
        synchronized(queue) {
            return queue.size();
        }
    }}
