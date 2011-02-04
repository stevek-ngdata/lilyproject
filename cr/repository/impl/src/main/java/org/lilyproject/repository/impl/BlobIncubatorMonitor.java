package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.zookeeper.*;

public class BlobIncubatorMonitor {
    private Log log = LogFactory.getLog(getClass());
    private final ZooKeeperItf zk;
    private LeaderElection leaderElection;
    private boolean stop;
    private final long minimalAge;
    private final long monitorDelay;
    private final BlobManager blobManager;
    private final TypeManager typeManager;
    private final HBaseTableFactory tableFactory;
    private Thread monitorThread;
    private HTableInterface recordTable;
    private HTableInterface blobIncubatorTable;
    private final long runDelay;

    public BlobIncubatorMonitor(ZooKeeperItf zk, HBaseTableFactory tableFactory, BlobManager blobManager, TypeManager typeManager, long minimalAge, long monitorDelay, long runDelay) {
        this.zk = zk;
        this.tableFactory = tableFactory;
        this.blobManager = blobManager;
        this.typeManager = typeManager;
        this.minimalAge = minimalAge;
        this.monitorDelay = monitorDelay;
        this.runDelay = runDelay;
    }
    
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        String electionPath = "/lily/repository/blobincubatormonitor";
        leaderElection = new LeaderElection(zk, "Blob Incubator Monitor",
                electionPath, new MyLeaderElectionCallback(this));
    }

    public void stop() {
        if (leaderElection != null) {
            try {
                leaderElection.stop();
                leaderElection = null;
            } catch (InterruptedException e) {
                log.info("Interrupted while shutting down leader election.");
            }
        }
    }
    
    public void startMonitoring() throws InterruptedException, IOException {
        this.blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(tableFactory, false);
        this.recordTable = LilyHBaseSchema.getRecordTable(tableFactory);
        synchronized (this) {
            stop = false;
            monitorThread = new Thread() {
                public void run() {
                    while (!stop) {
                        try {
                            try {
                                monitor();
                            } catch (IOException e) {
                                log.warn("Failed monitoring BlobIncubatorTable", e);
                                break;
                            }
                            if (stop)
                                break;
                            Thread.sleep(runDelay);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            };
        }
        monitorThread.start();
    }
    
    public void stopMonitoring() {
        synchronized (this) {
            stop = true;
            try {
                monitorThread.join();
            } catch (InterruptedException e) {
            }
            monitorThread = null;
        }
    }
    
    private void monitor() throws IOException, InterruptedException {
        Scan scan = new Scan();
        scan.addFamily(LilyHBaseSchema.BlobIncubatorCf.REF.bytes);
        long maxStamp = System.currentTimeMillis() - minimalAge;
        scan.setTimeRange(0, maxStamp);
        ResultScanner scanner = blobIncubatorTable.getScanner(scan);
        while (!stop) {
            Result[] results = scanner.next(100);
            if (results == null || (results.length == 0)) {
               break; 
            }
            for (Result result : results) {
                checkResult(result);
                if (stop) {
                    break;
                }
                Thread.sleep(monitorDelay);
            }
        }
    }
    
    private void checkResult(Result result) throws IOException, InterruptedException {
        byte[] recordId = result.getValue(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.RECORD.bytes);
        byte[] blobKey = result.getRow();
        if (Arrays.equals(recordId,BlobManagerImpl.INCUBATE)) {
                deleteBlob(blobKey);
        } else {
            byte[] fieldId = result.getValue(LilyHBaseSchema.BlobIncubatorCf.REF.bytes, LilyHBaseSchema.BlobIncubatorColumn.FIELD.bytes);
            Result blobUsage;
            try {
                blobUsage = getBlobUsage(blobKey, recordId, fieldId);
                if (blobUsage == null || blobUsage.isEmpty()) {
                    deleteBlob(blobKey); // Delete blob and reference
                } else {
                    deleteReference(blobKey, null); // The blob is used: only delete the reference
                }
            } catch (FieldTypeNotFoundException e) {
                log.warn("Failed to check blob usage " + blobKey + ", recordId " + recordId + ", fieldId " + fieldId, e);
            } catch (TypeException e) {
                log.warn("Failed to check blob usage " + blobKey + ", recordId " + recordId + ", fieldId " + fieldId, e);
            }
        }
    }
    
    private void deleteBlob(byte[] blobKey) throws IOException {
        RowLock rowLock = blobIncubatorTable.lockRow(blobKey);
        try {
            blobManager.delete(blobKey);
            deleteReference(blobKey, rowLock);
        } catch (BlobException e) {
            log.warn("Failed to delete blob " + blobKey, e);
            return;
        } finally {
            blobIncubatorTable.unlockRow(rowLock);
        }
    }

    private void deleteReference(byte[] blobKey, RowLock rowLock) throws IOException {
        Delete delete = new Delete(blobKey, System.currentTimeMillis(), rowLock);
        blobIncubatorTable.delete(delete);
    }
    
    private Result getBlobUsage(byte[] blobKey, byte[] recordId, byte[] fieldId) throws FieldTypeNotFoundException, TypeException, InterruptedException, IOException {
        FieldType fieldType = typeManager.getFieldTypeById(fieldId);
        ValueType valueType = fieldType.getValueType();
        Get get = new Get(recordId);
        get.addColumn(LilyHBaseSchema.RecordCf.DATA.bytes, fieldId);
        byte[] valueToCompare;
        if (valueType.isMultiValue() && valueType.isHierarchical()) {
            valueToCompare = Bytes.toBytes(2);
        } else if (valueType.isMultiValue() || valueType.isHierarchical()) {
            valueToCompare = Bytes.toBytes(1);
        } else {
            valueToCompare = Bytes.toBytes(0);
        }
        valueToCompare = Bytes.add(valueToCompare, blobKey);
        WritableByteArrayComparable valueComparator = new ContainsValueComparator(valueToCompare);
        Filter filter = new ValueFilter(CompareOp.EQUAL, valueComparator);
        get.setFilter(filter);
        return recordTable.get(get);
    }
    
    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        private final BlobIncubatorMonitor blobIncubatorMonitor;

        public MyLeaderElectionCallback(BlobIncubatorMonitor blobIncubatorMonitor) {
            this.blobIncubatorMonitor = blobIncubatorMonitor;
        }
        
        public void activateAsLeader() throws Exception {
            blobIncubatorMonitor.startMonitoring();
        }

        public void deactivateAsLeader() throws Exception {
            blobIncubatorMonitor.stopMonitoring();
        }
    }
    
}
