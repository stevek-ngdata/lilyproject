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
package org.lilyproject.hbaseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.util.hbase.LocalHTable;

/**
 * Simple migration tool to migrate from the situation where we stored the index definition (json) in a separate
 * index meta table to the situation where we store the index definition in a custom attribute on the
 * hbase table descriptor.
 */
public class IndexMetaTableMigrationTool {
    public static void main(String[] args) throws Exception {
        String zkConnect = System.getProperty("zookeeper");
        String metaTable = System.getProperty("table");

        System.out.println("Using zookeeper connect string " + zkConnect);
        System.out.println("Using index meta table " + metaTable);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnect);

        HTableInterface table = new LocalHTable(conf, metaTable);

        HBaseAdmin admin = new HBaseAdmin(conf);

        ResultScanner scanner = table.getScanner(Bytes.toBytes("meta"));

        Result result;
        while ((result = scanner.next()) != null) {
            byte[] tableName = result.getRow();
            String tableNameString = Bytes.toString(result.getRow());

            HTableDescriptor tableDescr;
            try {
                tableDescr = admin.getTableDescriptor(tableName);
            } catch (TableNotFoundException e) {
                System.out.println("Skipping non-existing index table: " + tableNameString);
                continue;
            }

            byte[] json = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("conf"));
            if (json == null) {
                System.out.println("Did not find KV containing index definition, skipping row: " + tableNameString);
                continue;
            }

            tableDescr.setValue(Bytes.toBytes("LILY_INDEX"), json);

            System.out.println("Disabling index table " + tableNameString);
            admin.disableTable(tableName);
            System.out.println("Storing index meta on index table " + tableNameString);
            admin.modifyTable(tableName, tableDescr);
            System.out.println("Enabling index table " + tableNameString);
            admin.enableTable(tableName);
            System.out.println("--");
        }

        System.out.println("After verifying that everything is correctly performed,");
        System.out.println("you may delete the index meta table.");

        scanner.close();
    }
}
