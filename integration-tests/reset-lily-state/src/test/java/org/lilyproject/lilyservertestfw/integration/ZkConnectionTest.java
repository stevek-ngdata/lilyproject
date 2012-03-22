package org.lilyproject.lilyservertestfw.integration;

import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.util.io.Closer;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test verifies that we have the expected number of ZooKeeper connections at any time.
 *
 * <p>We test the presence of ZK connections by looking if there are any threads with the
 * typical ZooKeeper name.</p>
 */
public class ZkConnectionTest {
    @Test
    public void testZkConnectionsGoneAfterLilyClientStop() throws Exception {
        
        // At this point, there should be no running ZK threads
        checkNoZkThread();

        LilyClient lilyClient = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);

        // Lily client is started, there should be (at least one) ZK thread
        checkZkThread();

        //System.out.println("Before close:");
        //printAllZkThreads();
        
        Closer.close(lilyClient);

        // We can't rely on threads being closed immediately
        int patience = 10;
        while (countZkThreads() > 0 && patience > 0) {
            Thread.sleep(1000);
            patience--;
        }
        
        //System.out.println("After close:");
        //printAllZkThreads();

        // Lily client is stopped, ZK threads should have been closed
        checkNoZkThread();
    }

    @Test
    public void testZkConnectionCount() throws Exception {
        // At this point, there should be no running ZK threads
        checkNoZkThread();

        LilyClient lilyClient = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);

        // What number of ZK connections we expect?
        //  1 for Lily
        //  1 for HBase (connection used for direct ops such as scanning, blobs)
        //  1 for HBaseAdmin
        int threadCnt = countZkThreads();
        assertEquals(3, threadCnt);
        
        Closer.close(lilyClient);
    }

    private void checkNoZkThread() {
        String name = findZkThread();
        if (name != null) {
            fail("Found running ZooKeeper thread: " + name);
        }
    }

    private void checkZkThread() {
        String name = findZkThread();
        if (name == null) {
            fail("Found NO running ZooKeeper thread.");
        }
    }

    private String findZkThread() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();
        for (long tid: threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(tid);
            
            String name = info.getThreadName();
            
            if (name.contains("-SendThread")) {
                return name;
            }
        }
        return null;
    }

    private int countZkThreads() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();
        int count = 0;
        for (long tid: threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(tid);
            String name = info.getThreadName();

            if (name.contains("-SendThread")) {
                count++;
            }
        }
        return count;
    }

    private void printAllZkThreads() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();
        for (long tid: threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(tid);

            String name = info.getThreadName();

            if (name.contains("-SendThread")) {
                System.out.println(name);
            }
        }
    }
}
