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
package org.lilycms.rowlog.impl.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class RowLogTableUtil {
    
    private static final String ROW_TABLE = "rowTable";
    
    public static final byte[] PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("PAYLOADCF");
    public static final byte[] EXECUTIONSTATE_COLUMN_FAMILY = Bytes.toBytes("ESLOGCF");

    public static HTable getRowTable(Configuration configuration) throws IOException {
        HTable rowTable;
        try {
            rowTable = new HTable(configuration, ROW_TABLE); 
        } catch (TableNotFoundException e) {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(ROW_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(PAYLOAD_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(EXECUTIONSTATE_COLUMN_FAMILY));
            admin.createTable(tableDescriptor);
            rowTable = new HTable(configuration, ROW_TABLE);
        }
        return rowTable;
    }
}
