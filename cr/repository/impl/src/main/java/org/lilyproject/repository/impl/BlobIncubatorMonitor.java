package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.hbaseext.ContainsValueComparator;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.Logs;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.zookeeper.*;

import static org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorColumn;

public class BlobIncubatorMonitor {
    private Log log = LogFactory.getLog(getClass());
    private BlobIncubatorMetrics metrics = new BlobIncubatorMetrics();
    private final ZooKeeperItf zk;
    private LeaderElection leaderElection;
    private final long minimalAge;
    private final long monitorDelay;
    private final BlobManager blobManager;
    private final TypeManager typeManager;
    private MonitorThread monitorThread;
    private HTableInterface recordTable;
    private HTableInterface blobIncubatorTable;
    private final long runDelay;

    public BlobIncubatorMonitor(ZooKeeperItf zk, HBaseTableFactory tableFactory, BlobManager blobManager,
            TypeManager typeManager, long minimalAge, long monitorDelay, long runDelay) throws IOException {
        this.zk = zk;
        this.blobManager = blobManager;
        this.typeManager = typeManager;
        this.minimalAge = minimalAge;
        this.monitorDelay = monitorDelay;
        this.runDelay = runDelay;

        this.blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(tableFactory, false);
        this.recordTable = LilyHBaseSchema.getRecordTable(tableFactory);
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
    
    public synchronized void startMonitoring() throws InterruptedException, IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
    }
    
    public synchronized void stopMonitoring() {
        if (monitorThread != null) {
            monitorThread.shutdown();
            try {
                if (monitorThread.isAlive()) {
                    Logs.logThreadJoin(monitorThread);
                    monitorThread.join();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
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
 
    private class MonitorThread extends Thread {
        private boolean stopRequested = false;

        public MonitorThread() {
        }

        @Override
        public synchronized void start() {
            stopRequested = false;
            super.start();
        }
        
        public void shutdown() {
            stopRequested = true;
            interrupt();
        }
        
        public void run() {
            while (!stopRequested) {
                try {
                    try {
                        monitor();
                    } catch (IOException e) {
                        log.warn("Failed monitoring BlobIncubatorTable", e);
                        break;
                    }
                    if (stopRequested)
                        break;
                    Thread.sleep(runDelay);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        public void monitor() throws IOException, InterruptedException {
            log.debug("Start run blob incubator monitor");
            long monitorBegin = System.currentTimeMillis();
            Scan scan = new Scan();
            scan.addFamily(BlobIncubatorCf.REF.bytes);
            long maxStamp = System.currentTimeMillis() - minimalAge;
            scan.setTimeRange(0, maxStamp);
            ResultScanner scanner = blobIncubatorTable.getScanner(scan);
            while (!stopRequested) {
                Result[] results = scanner.next(100);
                if (results == null || (results.length == 0)) {
                   break;
                }
                for (Result result : results) {
                    long before = System.currentTimeMillis();
                    checkResult(result);
                    // usually these times will be very short, a bit too short to measure with ms precision, but
                    // this is mainly to observe when it would take long so that is fine
                    metrics.checkDuration.inc(System.currentTimeMillis() - before);

                    if (stopRequested) {
                        break;
                    }

                    if (monitorDelay > 0) {
                        Thread.sleep(monitorDelay);
                    }
                }
            }
            metrics.runDuration.inc(System.currentTimeMillis() - monitorBegin);
            log.debug("Stop run blob incubator monitor");
        }

        private void checkResult(Result result) throws IOException, InterruptedException {
            byte[] recordId = result.getValue(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes);
            byte[] blobKey = result.getRow();
            if (Arrays.equals(recordId,BlobManagerImpl.INCUBATE)) {
                    deleteBlob(blobKey, recordId, null);
            } else {
                byte[] fieldId = result.getValue(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.FIELD.bytes);
                Result blobUsage;
                try {
                    blobUsage = getBlobUsage(blobKey, recordId, fieldId);
                    if (blobUsage == null || blobUsage.isEmpty()) {
                        deleteBlob(blobKey, recordId, fieldId); // Delete blob and reference
                    } else {
                        deleteReference(blobKey, recordId); // The blob is used: only delete the reference
                    }
                } catch (FieldTypeNotFoundException e) {
                    log.warn("Failed to check blob usage " + Hex.encodeHexString(blobKey) +
                            ", recordId " + Hex.encodeHexString(recordId) +
                            ", fieldId " + Hex.encodeHexString(fieldId), e);
                } catch (TypeException e) {
                    log.warn("Failed to check blob usage " + Hex.encodeHexString(blobKey) +
                            ", recordId " + Hex.encodeHexString(recordId) +
                            ", fieldId " + Hex.encodeHexString(fieldId), e);
                }
            }
        }

        private void deleteBlob(byte[] blobKey, byte[] recordId, byte[] fieldId) throws IOException {
            if (deleteReference(blobKey, recordId)) {
                try {
                    blobManager.delete(blobKey);
                } catch (BlobException e) {
                    log.warn("Failed to delete blob " + blobKey, e);
                    // Deleting the blob failed. We put back the reference to try it again later.
                    // There's a small chance that this fails as well. In that there will be an unreferenced blob in the blobstore.
                    Put put = new Put(blobKey);
                    put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes, recordId);
                    if (fieldId != null) {
                        put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.FIELD.bytes, fieldId);
                    }
                    blobIncubatorTable.put(put);
                    return;
                }
            }
        }

        private boolean deleteReference(byte[] blobKey, byte[] recordId) throws IOException {
            Delete delete = new Delete(blobKey);
            return blobIncubatorTable.checkAndDelete(blobKey, BlobIncubatorCf.REF.bytes,
                    BlobIncubatorColumn.RECORD.bytes, recordId, delete);
        }

        private Result getBlobUsage(byte[] blobKey, byte[] recordId, byte[] fieldId) throws FieldTypeNotFoundException,
                TypeException, InterruptedException, IOException {
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
    }

    /**
     * Runs the monitor once on the current thread. Should not be called if the cleanup might already be running
     * on another thread (i.e. {@link #start} should not have been called).
     */
    public void runMonitorOnce() throws IOException, InterruptedException {
        new MonitorThread().monitor();
    }

}
