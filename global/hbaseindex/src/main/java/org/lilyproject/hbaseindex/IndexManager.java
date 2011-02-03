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
package org.lilyproject.hbaseindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LocalHTable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Starting point for all the index and query functionality.
 *
 * <p>This class should be instantiated yourself. This class is threadsafe,
 * but on the other hand rather lightweight so it does not harm to have multiple
 * instances.
 */
public class IndexManager {
    private Configuration hbaseConf;
    private HBaseAdmin hbaseAdmin;
    private HTableInterface metaTable;
    private HBaseTableFactory tableFactory;
    private final Log log = LogFactory.getLog(getClass());

    public static final String DEFAULT_META_TABLE = "indexmeta";

    /**
     * Constructor.
     *
     * <p>Calls {@link #IndexManager(Configuration, String) IndexManager(hbaseConf, DEFAULT_META_TABLE)}.
     */
    public IndexManager(Configuration hbaseConf) throws IOException {
        this(hbaseConf, DEFAULT_META_TABLE);
    }

    /**
     * Constructor.
     *
     * <p>The supplied metaTableName should be an existing table. You can use the utility
     * method {@link #createIndexMetaTable} to create this table.
     *
     * @param metaTableName name of the HBase table in which to manage the configuration of the indexes
     */
    public IndexManager(Configuration hbaseConf, String metaTableName) throws IOException {
        this(hbaseConf, metaTableName, null);
    }

    public IndexManager(Configuration hbaseConf, String metaTableName, HBaseTableFactory tableFactory) throws IOException {
        this.hbaseConf = hbaseConf;
        hbaseAdmin = new HBaseAdmin(hbaseConf);
        metaTable = new LocalHTable(hbaseConf, metaTableName);
        this.tableFactory = tableFactory != null ? tableFactory : new HBaseTableFactoryImpl(hbaseConf);
    }

    public synchronized void createIndex(IndexDefinition indexDef) throws IOException, InterruptedException {
        createIndex(indexDef, null);
    }

    /**
     * Creates a new index.
     *
     * <p>This first creates the HBase table for this index, then adds the index
     * definition to the indexmeta table.
     *
     * @throws IndexExistsException if an index with the same name already exists
     */
    public synchronized void createIndex(IndexDefinition indexDef, byte[][] splitKeys)
            throws IOException, InterruptedException {
        if (indexDef.getFields().size() == 0) {
            throw new IllegalArgumentException("An IndexDefinition should contain at least one field.");
        }

        byte[] jsonData = serialize(indexDef);

        HTableDescriptor table = new HTableDescriptor(indexDef.getName());
        HColumnDescriptor family = new HColumnDescriptor(Index.DATA_FAMILY, 1, HColumnDescriptor.DEFAULT_COMPRESSION,
                HColumnDescriptor.DEFAULT_IN_MEMORY, HColumnDescriptor.DEFAULT_BLOCKCACHE, HColumnDescriptor.DEFAULT_BLOCKSIZE,
                HColumnDescriptor.DEFAULT_TTL, HColumnDescriptor.DEFAULT_BLOOMFILTER, HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
        table.addFamily(family);

        tableFactory.configure(table);

        if (splitKeys == null) {
            splitKeys = tableFactory.getSplitKeys(table.getName());
        }

        try {
            hbaseAdmin.createTable(table, splitKeys);
        } catch (TableExistsException e) {
            // This could be because:
            //   * the index simply already exists
            //   * the index is in the process of being created concurrently by another process (a typical scenario
            //     when starting the same process concurrently on multiple nodes). In this case, after table creation
            //     the metaTable still needs to be updated, therefore the loop below to give this some time.

            Get get = new Get(Bytes.toBytes(indexDef.getName()));
            Result result = metaTable.get(get);
            int count = 0;
            while (result.isEmpty() && count < 30) {
                Thread.sleep(100);
                count++;
                result = metaTable.get(get);
            }

            if (result.isEmpty()) {
                // The table exists but not as an index: throw the original TableExistsException
                throw e;
            } else {
                throw new IndexExistsException(indexDef.getName());
            }
        }

        Put put = new Put(Bytes.toBytes(indexDef.getName()));
        put.add(Bytes.toBytes("meta"), Bytes.toBytes("conf"), jsonData);
        metaTable.put(put);

        log.info("Created index table '" + indexDef.getName() + "' using " +
                (splitKeys == null ? 1 : splitKeys.length + 1) + " initial regions.");
    }

    public synchronized void createIndexIfNotExists(IndexDefinition indexDef) throws IOException, InterruptedException {
        createIndexIfNotExists(indexDef, null);
    }

    public synchronized void createIndexIfNotExists(IndexDefinition indexDef, byte[][] splitKeys) throws IOException,
            InterruptedException {

        Get get = new Get(Bytes.toBytes(indexDef.getName()));
        Result result = metaTable.get(get);
        boolean indexExisted = !result.isEmpty();

        if (!indexExisted) {
            try {
                createIndex(indexDef, splitKeys);
            } catch (IndexExistsException e) {
                indexExisted = true;
            }
        }

        if (indexExisted) {
            result = metaTable.get(get); // would still have been empty in case of IndexExistsException
            Index index = instantiateIndex(indexDef.getName(), result);
            if (!index.getDefinition().equals(indexDef)) {
                throw new RuntimeException("Index " + indexDef.getName() +
                        " exists but its definition does not match the supplied definition.");
            }
        }
    }

    private byte[] serialize(IndexDefinition indexDef) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(os, indexDef.toJson());
        return os.toByteArray();
    }

    private IndexDefinition deserialize(String name, byte[] jsonData) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return new IndexDefinition(name, mapper.readValue(jsonData, 0, jsonData.length, ObjectNode.class));
    }

    /**
     * Retrieves an Index.
     *
     * @throws IndexNotFoundException if the index does not exist
     */
    public Index getIndex(String name) throws IOException, IndexNotFoundException {
        Get get = new Get(Bytes.toBytes(name));
        Result result = metaTable.get(get);

        if (result.isEmpty())
            throw new IndexNotFoundException(name);

        return instantiateIndex(name, result);
    }

    private Index instantiateIndex(String name, Result result) throws IOException {
        byte[] jsonData = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("conf"));
        IndexDefinition indexDef = deserialize(name, jsonData);

        HTableInterface htable = new LocalHTable(hbaseConf, name);
        Index index = new Index(htable, indexDef);
        return index;
    }

    /**
     * Deletes an index.
     *
     * <p>This removes the index definition from the index meta table, disables the
     * index table and deletes it. If this would fail in between any of these operations,
     * it is up to the administrator to perform the remaining work.
     *
     * @throws IndexNotFoundException if the index does not exist.
     */
    public synchronized void deleteIndex(String name) throws IOException, IndexNotFoundException {
        Get get = new Get(Bytes.toBytes(name));
        Result result = metaTable.get(get);

        if (result.isEmpty())
            throw new IndexNotFoundException(name);

        // TODO what if this fails in between operations? Log this...

        Delete del = new Delete(Bytes.toBytes(name));
        metaTable.delete(del);

        hbaseAdmin.disableTable(name);
        hbaseAdmin.deleteTable(name);
    }

    /**
     * Utility method for creating the indexmeta table.
     */
    public static void createIndexMetaTable(Configuration hbaseConf, String metaTableName) throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
        HTableDescriptor table = new HTableDescriptor(metaTableName);
        HColumnDescriptor family = new HColumnDescriptor("meta");
        table.addFamily(family);
        hbaseAdmin.createTable(table);
    }

    public static void createIndexMetaTable(Configuration hbaseConf) throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
        HTableDescriptor table = new HTableDescriptor(DEFAULT_META_TABLE);
        HColumnDescriptor family = new HColumnDescriptor("meta");
        table.addFamily(family);
        hbaseAdmin.createTable(table);
    }

    public static void createIndexMetaTableIfNotExists(Configuration hbaseConf) throws IOException {
        try {
            createIndexMetaTable(hbaseConf);
        } catch (TableExistsException e) {
            // ignore
        }
    }
}
