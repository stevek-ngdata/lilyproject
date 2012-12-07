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
package org.lilyproject.tools.upgrade;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.kauriproject.conf.Conf;
import org.kauriproject.conf.XmlConfBuilder;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.compat.Lily11RecordIdDecoder;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.server.modules.general.TableConfigBuilder;
import org.lilyproject.util.Version;
import org.lilyproject.util.hbase.*;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.util.*;

import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class UpgradeFrom1_1Tool extends BaseZkCliTool {
    private ZooKeeperItf zk;
    private final String SRC_TABLE_NAME = "record";
    private IdGeneratorImpl idGenerator = new IdGeneratorImpl();
    
    private boolean writeToWal = false;
    private long writeBufferSize = 1024 * 1024 * 10;
    private String destTableName = "record_lily_1_2";
    private String tableConfFileName;

    private Option confirmOption;
    private Option tableConfOption;
    private Option writeToWalOption;
    private Option destTableOption;

    @Override
    protected String getCmdName() {
        return "lily-upgrade-from-1.1";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-upgrade");
    }

    public static void main(String[] args) {
        new UpgradeFrom1_1Tool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        confirmOption = OptionBuilder
                .withDescription("Confirm you want to start the upgrade.")
                .create("confirm");
        options.add(confirmOption);

        tableConfOption = OptionBuilder
                        .withArgName("filename")
                        .hasArg()
                        .withDescription("Table creation options file, like conf/general/tables.xml")
                        .withLongOpt("table-options")
                        .create("to");
        options.add(tableConfOption);

        destTableOption = OptionBuilder
                .withArgName("tablename")
                .hasArg()
                .withDescription("Destination table name, default " + destTableName)
                .withLongOpt("table-name")
                .create("tn");
        options.add(destTableOption);

        writeToWalOption = OptionBuilder
                .withDescription("Enable write to WAL, off by default.")
                .withLongOpt("write-to-wal")
                .create("wtw");
        options.add(writeToWalOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;


        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);

        if (!assertLilyNotRunning()) {
            return 1;
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);

        HBaseAdmin admin = new HBaseAdmin(conf);

        tableConfFileName = cmd.getOptionValue(tableConfOption.getOpt());
        writeToWal = cmd.hasOption(writeToWalOption.getOpt());
        destTableName = cmd.hasOption(destTableOption.getOpt()) ? cmd.getOptionValue(destTableOption.getOpt()) : destTableName;

        System.out.println("Using settings:");
        System.out.println("  ZooKeeper: " + zkConnectionString);
        System.out.println("  Write buffer size: " + writeBufferSize);
        System.out.println("  Write to WAL: " + writeToWal);
        System.out.println("  Table configuration file: " + tableConfFileName);
        System.out.println("  Destination table name: " + destTableName);
        System.out.println();
        
        if (!cmd.hasOption(confirmOption.getOpt())) {
            System.out.println("Please supply the -confirm option to start the upgrade.");
            return 1;
        }

        //
        // Source table
        //
        HTable srcTable;
        try {
            srcTable = new HTable(conf, SRC_TABLE_NAME);
        } catch (TableNotFoundException e) {
            System.err.println("Source table does not exist: " + SRC_TABLE_NAME);
            return 1;
        }

        //
        // Destination table
        //
        if (admin.tableExists(destTableName)) {
            System.err.println("Destination table already exists, please drop it or specify another table name: "
                    + destTableName);
            return 1;            
        }

        System.out.println("Creating destination table " + destTableName);
        
        HBaseTableFactory tableFactory;
        if (tableConfFileName != null) {        
            Conf tableConf = XmlConfBuilder.build(new File(tableConfFileName));        
            List<TableConfigEntry> tableConfs = TableConfigBuilder.buildTableConfigs(tableConf);
            ColumnFamilyConfig defaultFamilyConf = TableConfigBuilder.buildCfConfig(tableConf.getChild("familyDefaults"));
            tableFactory = new HBaseTableFactoryImpl(conf, tableConfs, defaultFamilyConf);
        } else {
            tableFactory = new HBaseTableFactoryImpl(conf);            
        }
        tableFactory.getTable(getRecordTableDescriptor(destTableName));

        // Use a plain HTable so that we can make use of the write buffer
        HTable destTable = new HTable(conf, destTableName);
        destTable.setAutoFlush(false);
        destTable.setWriteBufferSize(writeBufferSize);

        //
        // Start the real work
        //        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setMaxVersions();
        ResultScanner scanner = srcTable.getScanner(scan);
        
        int cnt = 0;
        int del = 0; // count of deleted records
        
        System.out.println("Starting...");
        for (Result row : scanner) {
            // Check if the record is marked as deleted, if so, don't bother copying it over
            byte[] deleted = row.getValue(RecordCf.DATA.bytes, RecordColumn.DELETED.bytes);
            if (Bytes.toBoolean(deleted)) {
                del++;

                if ((del % 1000) == 0) {
                    System.out.println("Skipped deleted records: " + del);
                }

                continue;
            }
            
            RecordId recordId = Lily11RecordIdDecoder.decode(new DataInputImpl(row.getRow()), idGenerator);

            Put put = new Put(recordId.toBytes());
            
            put.setWriteToWAL(writeToWal);

            for (KeyValue kv : row.raw()) {
                put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
            }
            
            destTable.put(put);
            
            cnt++;
            
            if ((cnt % 1000) == 0) {
                System.out.println("Copied records: " + cnt);
            }
        }
        
        destTable.flushCommits();

        System.out.println("Done!");
        System.out.println("");
        System.out.println("Total counts:");
        System.out.println("  Copied records: " + cnt);
        System.out.println("  Skipped deleted records: " + del);
        System.out.println("");

        if (!writeToWal) {
            System.out.println("Since write to wal is false, now triggering a flush on HBase (async operation)");
            admin.flush(destTableName);
        }
        
        System.out.println();
        System.out.println("To finish the upgrade, perform the following operations from the HBase shell:");
        System.out.println("disable '" + destTableName + "'");
        System.out.println("disable '" + SRC_TABLE_NAME + "'");
        System.out.println("drop '" + SRC_TABLE_NAME + "'");
        System.out.println();
        System.out.println("Now rename the new table to " + SRC_TABLE_NAME + " using this command:");
        System.out.println("bin/hbase org.jruby.Main bin/rename_table.rb " + destTableName + " " + SRC_TABLE_NAME);
        System.out.println();
        System.out.println("Again in the HBase shell, disable and re-enable the table using:");
        System.out.println("disable '" + SRC_TABLE_NAME + "'");
        System.out.println("enable '" + SRC_TABLE_NAME + "'");
        System.out.println();
        System.out.println("To check things work, you might want to do a:");
        System.out.println("count '" + SRC_TABLE_NAME + "'");

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(zk);
        HConnectionManager.deleteAllConnections(true);
        super.cleanup();
    }
    
    private HTableDescriptor getRecordTableDescriptor(String tableName) {
        // Copied from LilyHBaseSchema to allow for changing table name
        HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableName));
        descriptor.addFamily(new HColumnDescriptor(RecordCf.DATA.bytes,
                HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
        descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("rowlog")));
        return descriptor;
    }

    private boolean assertLilyNotRunning() throws Exception {
        List<String> lilyServers = zk.getChildren("/lily/repositoryNodes", true);
        if (!lilyServers.isEmpty()) {
            System.out.println("WARNING! Lily should not be running when performing this conversion.");
            System.out.println("         Only HBase, Hadoop and zookeeper should be running.");
            System.out.println("         Running Lily servers:");
            for (String server : lilyServers) {
                System.out.println("           " + server);
            }
            return false;
        }
        return true;
    }
}
