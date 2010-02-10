package org.lilycms.hbaseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexManager {
    private Configuration hbaseConf;
    private HBaseAdmin hbaseAdmin;
    private Map<String, IndexDefinition> indexDefs = new HashMap<String, IndexDefinition>();

    public IndexManager(Configuration hbaseConf) throws MasterNotRunningException {
        this.hbaseConf = hbaseConf;
        hbaseAdmin = new HBaseAdmin(hbaseConf);
    }

    public void createCompositeIndex(IndexDefinition indexDef) throws IOException {
        HTableDescriptor table = new HTableDescriptor(indexDef.getName());
        HColumnDescriptor family = new HColumnDescriptor("dummy");
        table.addFamily(family);
        hbaseAdmin.createTable(table);
        indexDefs.put(indexDef.getName(), indexDef);
    }

    public Index getIndex(String name) throws IOException {
        HTable htable = new HTable(hbaseConf, name);
        Index index = new Index(htable, indexDefs.get(name));
        return index;
    }
}
