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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.util.ObjectUtils;
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
    private HBaseTableFactory tableFactory;
    private static final byte[] INDEX_META_KEY = Bytes.toBytes("LILY_INDEX");

    /**
     * Constructor.
     */
    public IndexManager(Configuration hbaseConf) throws IOException {
        this(hbaseConf, null);
    }

    public IndexManager(Configuration hbaseConf, HBaseTableFactory tableFactory) throws IOException {
        this.hbaseConf = hbaseConf;
        hbaseAdmin = new HBaseAdmin(hbaseConf);
        this.tableFactory = tableFactory != null ? tableFactory : new HBaseTableFactoryImpl(hbaseConf);
    }

    /**
     * Creates a new index.
     *
     * @throws IndexExistsException if an index with the same name already exists
     */
    public synchronized Index getIndex(IndexDefinition indexDef) throws IOException, InterruptedException {
        if (indexDef.getFields().size() == 0) {
            throw new IllegalArgumentException("An IndexDefinition should contain at least one field.");
        }

        byte[] jsonData = serialize(indexDef);

        HTableDescriptor tableDescr = new HTableDescriptor(indexDef.getName());
        HColumnDescriptor family = new HColumnDescriptor(Index.DATA_FAMILY, 1, HColumnDescriptor.DEFAULT_COMPRESSION,
                HColumnDescriptor.DEFAULT_IN_MEMORY, HColumnDescriptor.DEFAULT_BLOCKCACHE,
                HColumnDescriptor.DEFAULT_BLOCKSIZE, HColumnDescriptor.DEFAULT_TTL,
                HColumnDescriptor.DEFAULT_BLOOMFILTER, HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
        tableDescr.addFamily(family);

        // Store definition of index in a custom attribute on the table
        tableDescr.setValue(INDEX_META_KEY, jsonData);

        HTableInterface table = tableFactory.getTable(tableDescr);

        byte[] actualMeta = table.getTableDescriptor().getValue(INDEX_META_KEY);
        if (!ObjectUtils.safeEquals(jsonData, actualMeta)) {
            throw new RuntimeException("Index " + indexDef.getName() +
                    " exists but its definition does not match the supplied definition.");
        }

        return instantiateIndex(indexDef.getName(), table);
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
        HTableInterface table;
        try {
            table = new LocalHTable(hbaseConf, name);
        } catch (TableNotFoundException e) {
            throw new IndexNotFoundException(name);
        }

        return instantiateIndex(name, table);
    }

    private Index instantiateIndex(String name, HTableInterface table) throws IOException {
        byte[] jsonData = table.getTableDescriptor().getValue(INDEX_META_KEY);
        if (jsonData == null) {
            throw new IOException("Not a valid index table: " + name);
        }

        IndexDefinition indexDef = deserialize(name, jsonData);

        Index index = new Index(table, indexDef);
        return index;
    }

    /**
     * Deletes an index.
     *
     * <p>This disables the index table and deletes it.
     *
     * @throws IndexNotFoundException if the index does not exist.
     */
    public synchronized void deleteIndex(String name) throws IOException, IndexNotFoundException {
        HTableDescriptor tableDescr;

        try {
            tableDescr = hbaseAdmin.getTableDescriptor(Bytes.toBytes(name));
        } catch (TableNotFoundException e) {
            throw new IndexNotFoundException(name);
        }

        if (tableDescr.getValue(INDEX_META_KEY) == null) {
            throw new IOException("Table exists but is not an index table: " + name);
        }

        hbaseAdmin.disableTable(name);
        hbaseAdmin.deleteTable(name);
    }
}
