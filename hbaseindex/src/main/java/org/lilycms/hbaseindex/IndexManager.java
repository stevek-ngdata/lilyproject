package org.lilycms.hbaseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class IndexManager {
    private Configuration hbaseConf;
    private HBaseAdmin hbaseAdmin;
    private HTable metaTable;

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
        this.hbaseConf = hbaseConf;
        hbaseAdmin = new HBaseAdmin(hbaseConf);
        metaTable = new HTable(hbaseConf, metaTableName);
    }

    public void createIndex(IndexDefinition indexDef) throws IOException {
        byte[] jsonData = serialize(indexDef);

        HTableDescriptor table = new HTableDescriptor(indexDef.getName());
        HColumnDescriptor family = new HColumnDescriptor("dummy");
        table.addFamily(family);
        hbaseAdmin.createTable(table);

        Put put = new Put(Bytes.toBytes(indexDef.getName()));
        put.add(Bytes.toBytes("meta"), Bytes.toBytes("conf"), jsonData);
        metaTable.put(put);
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

    public Index getIndex(String name) throws IOException, IndexNotFoundException {
        Get get = new Get(Bytes.toBytes(name));
        Result result = metaTable.get(get);

        if (result.isEmpty())
            throw new IndexNotFoundException(name);

        byte[] jsonData = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("conf"));
        IndexDefinition indexDef = deserialize(name, jsonData);

        HTable htable = new HTable(hbaseConf, name);
        Index index = new Index(htable, indexDef);
        return index;
    }

    public void deleteIndex(String name) throws IOException, IndexNotFoundException {
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
}
