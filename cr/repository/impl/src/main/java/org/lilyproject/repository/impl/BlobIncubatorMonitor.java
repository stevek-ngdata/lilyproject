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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.repository.impl.hbase.ContainsValueComparator;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.RepositoryTableManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.Logs;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.BlobIncubatorColumn;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.LeaderElection;
import org.lilyproject.util.zookeeper.LeaderElectionCallback;
import org.lilyproject.util.zookeeper.LeaderElectionSetupException;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

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
    private HTableInterface blobIncubatorTable;
    private HBaseTableFactory tableFactory;
    private RepositoryTableManager repositoryTableManager;
    private final long runDelay;

    public BlobIncubatorMonitor(ZooKeeperItf zk, HBaseTableFactory tableFactory, RepositoryTableManager repositoryTableManager,
            BlobManager blobManager, TypeManager typeManager, long minimalAge, long monitorDelay, long runDelay) throws IOException, InterruptedException {
        this.zk = zk;
        this.blobManager = blobManager;
        this.typeManager = typeManager;
        this.minimalAge = minimalAge;
        this.monitorDelay = monitorDelay;
        this.runDelay = runDelay;

        this.blobIncubatorTable = LilyHBaseSchema.getBlobIncubatorTable(tableFactory, false);

        this.tableFactory = tableFactory;
        this.repositoryTableManager = repositoryTableManager;
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

        @Override
        public void activateAsLeader() throws Exception {
            blobIncubatorMonitor.startMonitoring();
        }

        @Override
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

        @Override
        public void run() {
            while (!stopRequested) {
                try {
                    try {
                        monitor();
                    } catch (RepositoryException e) {
                        log.warn("Failed monitoring BlobIncubatorTable", e);
                        break;
                    } catch (IOException e) {
                        log.warn("Failed monitoring BlobIncubatorTable", e);
                        break;
                    }
                    if (stopRequested) {
                        break;
                    }
                    Thread.sleep(runDelay);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        public void monitor() throws IOException, RepositoryException, InterruptedException {
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
            Closer.close(scanner);
            metrics.runDuration.inc(System.currentTimeMillis() - monitorBegin);
            log.debug("Stop run blob incubator monitor");
        }

        private void checkResult(Result result) throws IOException, RepositoryException, InterruptedException {
            byte[] recordIdBytes = result.getValue(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes);
            SchemaId recordId = new SchemaIdImpl(recordIdBytes);
            byte[] blobKey = result.getRow();
            if (Arrays.equals(recordIdBytes,BlobManagerImpl.INCUBATE)) {
                    deleteBlob(blobKey, recordId, null);
            } else {
                SchemaId fieldId = new SchemaIdImpl(result.getValue(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.FIELD.bytes));
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
                            ", recordId " + recordId +
                            ", fieldId " + fieldId, e);
                } catch (TypeException e) {
                    log.warn("Failed to check blob usage " + Hex.encodeHexString(blobKey) +
                            ", recordId " + recordId +
                            ", fieldId " + fieldId, e);
                }
            }
        }

        private void deleteBlob(byte[] blobKey, SchemaId recordId, SchemaId fieldId) throws IOException {
            if (deleteReference(blobKey, recordId)) {
                try {
                    blobManager.delete(blobKey);
                    metrics.blobDeleteCount.inc();
                } catch (BlobException e) {
                    log.warn("Failed to delete blob " + Hex.encodeHexString(blobKey), e);
                    // Deleting the blob failed. We put back the reference to try it again later.
                    // There's a small chance that this fails as well. In that there will be an unreferenced blob in the blobstore.
                    Put put = new Put(blobKey);
                    put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.RECORD.bytes, recordId.getBytes());
                    if (fieldId != null) {
                        put.add(BlobIncubatorCf.REF.bytes, BlobIncubatorColumn.FIELD.bytes, fieldId.getBytes());
                    }
                    blobIncubatorTable.put(put);
                }
            }
        }

        private boolean deleteReference(byte[] blobKey, SchemaId recordId) throws IOException {
            Delete delete = new Delete(blobKey);
            boolean result = blobIncubatorTable.checkAndDelete(blobKey, BlobIncubatorCf.REF.bytes,
                    BlobIncubatorColumn.RECORD.bytes, recordId.getBytes(), delete);
            if (result) {
                metrics.refDeleteCount.inc();
            }
            return result;
        }

        private Result getBlobUsage(byte[] blobKey, SchemaId recordId, SchemaId fieldId) throws FieldTypeNotFoundException,
                TypeException, InterruptedException, IOException, RepositoryException {
            FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeById(fieldId);
            ValueType valueType = fieldType.getValueType();
            Get get = new Get(recordId.getBytes());
            get.addColumn(RecordCf.DATA.bytes, fieldType.getQualifier());
            byte[] valueToCompare = Bytes.toBytes(valueType.getNestingLevel());
            valueToCompare = Bytes.add(valueToCompare, blobKey);
            WritableByteArrayComparable valueComparator = new ContainsValueComparator(valueToCompare);
            Filter filter = new SingleColumnValueFilter(RecordCf.DATA.bytes, fieldType.getQualifier(), CompareOp.EQUAL, valueComparator);
            get.setFilter(filter);

            for (RepositoryTable repoTable : repositoryTableManager.getTables()) {
                HTableInterface recordTable = LilyHBaseSchema.getRecordTable(tableFactory, repoTable.getName());
                Result result = recordTable.get(get);
                if (result != null && !result.isEmpty()) {
                    return result;
                }
            }
            return null;
        }
    }

    /**
     * Runs the monitor once on the current thread. Should not be called if the cleanup might already be running
     * on another thread (i.e. {@link #start} should not have been called).
     */
    public void runMonitorOnce() throws IOException, RepositoryException, InterruptedException {
        new MonitorThread().monitor();
    }

}
