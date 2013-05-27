/*
 * Copyright 2013 NGDATA nv
 */
package org.lilyproject.tools.upgrade;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.util.Version;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;

public class UpgradeFrom2_0Tool extends BaseZkCliTool {

    private Option confirmOption;
    private StateWatchingZooKeeper zk;

    @Override
    protected String getCmdName() {
        return "lily-upgrade-from-2.0";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-upgrade");
    }

    public static void main(String[] args) {
        new UpgradeFrom2_0Tool().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        confirmOption = OptionBuilder.withDescription("Confirm you want to start the upgrade.").create("confirm");
        options.add(confirmOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {

        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        zk = new StateWatchingZooKeeper(zkConnectionString, zkSessionTimeout);

        if (!assertLilyNotRunning()) {
            return 1;
        }

        if (!cmd.hasOption(confirmOption.getOpt())) {
            System.out.println("Please supply the -confirm option to start the upgrade.");
            return 1;
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkConnectionString);

        upgradeTables(conf);

        return 0;
    }

    private void upgradeTables(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException,
            IOException, TableNotFoundException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        try {
            // Update the record table
            admin.disableTable(LilyHBaseSchema.Table.RECORD.bytes);
            HTableDescriptor recordTableDescriptor = new HTableDescriptor(
                    admin.getTableDescriptor(LilyHBaseSchema.Table.RECORD.bytes));
            recordTableDescriptor.removeFamily(Bytes.toBytes("rowlog"));
            HColumnDescriptor dataFamily = recordTableDescriptor.getFamily(LilyHBaseSchema.TypeCf.DATA.bytes);
            dataFamily.setScope(1);
            recordTableDescriptor.setValue(LilyHBaseSchema.TABLE_TYPE_PROPERTY, LilyHBaseSchema.TABLE_TYPE_RECORD);
            recordTableDescriptor.setValue("lilyOwningRepository", "default");
            admin.modifyTable(LilyHBaseSchema.Table.RECORD.bytes, recordTableDescriptor);
            admin.enableTable(LilyHBaseSchema.Table.RECORD.bytes);

            // Get rid of the rowlog tables
            if (admin.tableExists(Bytes.toBytes("rowlog-mq"))) {
                admin.disableTable(Bytes.toBytes("rowlog-mq"));
                admin.deleteTable(Bytes.toBytes("rowlog-mq"));
            }

            if (admin.tableExists(Bytes.toBytes("rowlog-wal"))) {
                admin.disableTable(Bytes.toBytes("rowlog-wal"));
                admin.deleteTable(Bytes.toBytes("rowlog-wal"));
            }
        } finally {
            admin.close();
        }
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
