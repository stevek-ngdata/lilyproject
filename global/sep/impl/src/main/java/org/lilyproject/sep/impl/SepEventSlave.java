package org.lilyproject.sep.impl;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.lilyproject.util.concurrent.WaitPolicy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class SepEventSlave extends BaseHRegionServer {
    private final String subscriptionId;
    private final EventListener listener;
    private final String hostName;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private RpcServer rpcServer;
    private final int threadCnt;
    private List<ThreadPoolExecutor> executors;
    private HashFunction hashFunction = Hashing.murmur3_32();
    private SepMetrics sepMetrics;
    private Log log = LogFactory.getLog(getClass());

    /**
     *
     * @param listener listeners that will process the events
     * @param hostName hostname to bind to
     */
    public SepEventSlave(String subscriptionId, EventListener listener, int threadCnt, String hostName, ZooKeeperItf zk,
            Configuration hbaseConf) {
        Preconditions.checkArgument(threadCnt > 0, "Thread count must be > 0");
        this.subscriptionId = subscriptionId;
        this.listener = listener;
        this.hostName = hostName;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.threadCnt = threadCnt;

        this.executors = new ArrayList<ThreadPoolExecutor>(threadCnt);
        for (int i = 0; i < threadCnt; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        // TODO see same call in HBase's HRegionServer:
        //   - should we do HBaseRPCErrorHandler ?
        rpcServer = HBaseRPC.getServer(this,
                new Class<?>[] { HRegionInterface.class },
                hostName,
                0, /* ephemeral port */
                10, // TODO how many handlers do we need? make it configurable?
                10,
                false, // TODO make verbose flag configurable
                hbaseConf,
                0); // TODO need to check what this parameter is for

        rpcServer.start();

        int port = rpcServer.getListenerAddress().getPort();

        // Publish our existence in ZooKeeper
        // See HBase ServerName class: format of server name is: host,port,startcode
        // Startcode is to distinguish restarted servers on same hostname/port
        String serverName = hostName + "," + port + "," + System.currentTimeMillis();
        zk.create(SepModel.HBASE_ROOT + "/" + subscriptionId + "/rs/" + serverName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        
        this.sepMetrics = new SepMetrics(subscriptionId);
    }

    public void stop() {
        Closer.close(rpcServer);
        sepMetrics.shutdown();
    }

    @Override
    public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
        // TODO quickly hacked in the multi-threading: should maybe approach this differently
        List<Future> futures = new ArrayList<Future>();

        // TODO Recording of last processed timestamp won't work if two batches of log entries are sent out of order
        long lastProcessedTimestamp = -1;
        
        nextEntry: for (HLog.Entry entry : entries) {
            for (final KeyValue kv : entry.getEdit().getKeyValues()) {
                if (kv.matchingColumn(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes)) {
                    // We don't want messages of the same row to be processed concurrently, therefore choose
                    // a thread based on the hash of the row key
                    int partition = (hashFunction.hashBytes(kv.getRow()).asInt() & Integer.MAX_VALUE) % threadCnt;
                    Future future = executors.get(partition).submit(new Runnable() {
                        @Override
                        public void run() {
                            long before = System.currentTimeMillis();
                            log.debug("Delivering message to listener");
                            listener.processMessage(kv.getRow(), kv.getValue());
                            sepMetrics.reportFilteredSepOperation(System.currentTimeMillis() - before); 
                        }
                    });
                    futures.add(future);
                    lastProcessedTimestamp = Math.max(lastProcessedTimestamp, entry.getKey().getWriteTime());
                    continue nextEntry;
                }
            }

            if (log.isInfoEnabled()) {
                // TODO this might not be unusual
                log.info("No payload found in " + entry.toString());
            }
        }

        // We should wait for all operations to finish before returning, because otherwise HBase might
        // deliver a next batch from the same HLog to a different server
        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in processing events.", e);
            } catch (Exception e) {
                // TODO think about error handling, probably just logging is a good idea
                log.error("Listener failure in processing event", e);
            }
        }
        if (lastProcessedTimestamp > 0) {
            sepMetrics.reportSepTimestamp(lastProcessedTimestamp);
        }
    }
}
