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
package org.lilyproject.client.integration;

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
 *
 * <p>This is implemented as an integration test, since if the Hadoop/Lily stack would be
 * started embedded, there would be extra ZK connections which would be hard to
 * distinguish from those set up by LilyClient.</p>
 *
 */
public class ZkConnectionTest {
    // For each ZK client, there are two threads, one with "-EventThread" in the name,
    // and one with "-SendThread" in the name. We arbitrarily use one of these.
    private static final String ZK_THREAD_MARKER = "-SendThread";

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
        //  [not applicable anymore since CDH3u4] 1 for HBaseAdmin
        int threadCnt = countZkThreads();
        assertEquals(2, threadCnt);

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

            if (name.contains(ZK_THREAD_MARKER)) {
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
            if (info == null) {
                continue;
            }
            String name = info.getThreadName();

            if (name.contains(ZK_THREAD_MARKER)) {
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

            if (name.contains(ZK_THREAD_MARKER)) {
                System.out.println(name);
            }
        }
    }
}
