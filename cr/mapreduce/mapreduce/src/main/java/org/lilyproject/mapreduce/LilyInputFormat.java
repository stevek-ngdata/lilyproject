package org.lilyproject.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.*;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LilyInputFormat extends InputFormat<RecordIdWritable, RecordWritable> implements Configurable {
    final Log log = LogFactory.getLog(LilyInputFormat.class);

    @Override
    public void setConf(Configuration entries) {
        // TODO
    }

    @Override
    public Configuration getConf() {
        // TODO
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        HTable table = null;
        ZooKeeperItf zk = null;
        Configuration hbaseConf = null;
        try {
            // TODO hardcoded localhost: user should supply ZK host via conf
            zk = ZkUtil.connect("localhost", 30000);
            hbaseConf = LilyClient.getHBaseConfiguration(zk);
            
            table = new HTable(hbaseConf, LilyHBaseSchema.Table.RECORD.bytes);
            return getSplits(table, new byte[0], new byte[0]);            
        } catch (ZkConnectException e) {
            throw new IOException("Error setting up splits", e);
        } finally {
            Closer.close(table);
            Closer.close(zk);
            if (hbaseConf != null) {
                HConnectionManager.deleteConnection(hbaseConf, true);
            }
        }
    }

    /**
     * License note: this code was copied from HBase's TableInputFormat.
     *
     * @param startRow start row of the scan
     * @param stopRow stop row of the scan
     */
    public List<InputSplit> getSplits(HTable table, final byte[] startRow, final byte[] stopRow) throws IOException {
        if (table == null) {
            throw new IOException("No table was provided.");
        }
        Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
        if (keys == null || keys.getFirst() == null ||
                keys.getFirst().length == 0) {
            throw new IOException("Expecting at least one region.");
        }
        int count = 0;
        List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
        for (int i = 0; i < keys.getFirst().length; i++) {
            if ( !includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
                continue;
            }
            String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
                    getServerAddress().getHostname();
            // determine if the given start an stop key fall into the region
            if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
                    Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
                    (stopRow.length == 0 ||
                            Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
                byte[] splitStart = startRow.length == 0 ||
                        Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
                        keys.getFirst()[i] : startRow;
                byte[] splitStop = (stopRow.length == 0 ||
                        Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
                        keys.getSecond()[i].length > 0 ?
                        keys.getSecond()[i] : stopRow;
                InputSplit split = new TableSplit(table.getTableName(),
                        splitStart, splitStop, regionLocation);
                splits.add(split);
                if (log.isDebugEnabled())
                    log.debug("getSplits: split -> " + (count++) + " -> " + split);
            }
        }
        return splits;
    }

    protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
        return true;
    }

    @Override
    public RecordReader<RecordIdWritable, RecordWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        // TODO hardcoded localhost
        LilyClient lilyClient = null;
        try {
            lilyClient = new LilyClient("localhost", 30000);
        } catch (Exception e) {
            throw new IOException("Error setting up LilyClient", e);
        }

        // TODO here we should deserialize the scan from the conf
        RecordScan scan = new RecordScan();

        // Change the start/stop record IDs on the scan to the current split
        TableSplit split = (TableSplit)inputSplit;
        scan.setRawStartRecordId(split.getStartRow());
        scan.setRawStopRecordId(split.getEndRow());

        // TODO disable cache blocks

        RecordScanner scanner = null;
        try {
            scanner = lilyClient.getRepository().getScanner(scan);
        } catch (RepositoryException e) {
            throw new IOException("Error setting up RecordScanner", e);
        }

        return new LilyRecordReader(lilyClient, scanner);
    }
}
