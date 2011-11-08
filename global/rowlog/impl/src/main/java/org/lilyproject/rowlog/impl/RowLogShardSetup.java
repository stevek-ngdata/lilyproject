package org.lilyproject.rowlog.impl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogShardList;
import org.lilyproject.util.hbase.HBaseTableFactory;

import java.io.IOException;

public class RowLogShardSetup {
    public static void setupShards(int shardCount, RowLog rowLog, HBaseTableFactory tableFactory) throws IOException {

        if (shardCount < 1 || shardCount > 255) {
            throw new IllegalArgumentException("Number of rowlog shards should be > 0 and < 255, but it is: "
                    + shardCount);
        }

        //
        // Create the rowlog table with its splits (if it does not exist yet)
        //
        byte[][] splits = new byte[shardCount - 1][];
        for (int i = 0; i < shardCount - 1 /* HBase adds last shard automatically (up to 'null' key) */ ; i++) {
            // region end keys are exclusive (everything lower than the end key is in the region)
            byte[] endKey = new byte[] { (byte)(i + 1) };
            splits[i] = endKey;
        }

        String tableName = "rowlog-" + rowLog.getId();
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        // Avoid any further splitting than the one we configured. The only reason to allow further splitting
        // is when the queue tables would get very large (due to messages not being consumed). But then we need
        // to deal with deleting splits afterwards if we don't want to end up with empty splits which would
        // negatively impact balancing. Therefore, for now, go for simple behavior: no further splitting.
        tableDescriptor.setMaxFileSize(Long.MAX_VALUE);

        tableDescriptor.addFamily(new HColumnDescriptor(RowLogShardImpl.MESSAGES_CF));

        HTableInterface table = tableFactory.getTable(tableDescriptor, splits);

        //
        // Create the RowLogShard instances
        //
        RowLogShardList shards = rowLog.getShardList();
        for (int i = 0; i < shardCount; i++) {
            byte[] rowKeyPrefix = new byte[] { (byte)i };
            shards.addShard(new RowLogShardImpl("shard" + i, rowKeyPrefix, table, rowLog, 100));
        }
    }
}
