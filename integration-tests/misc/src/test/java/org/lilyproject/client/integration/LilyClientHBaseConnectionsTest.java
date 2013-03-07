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

import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.util.io.Closer;

import static org.junit.Assert.assertEquals;

/**
 * This test verifies the proper lifecycle of HBase connections used by LilyClient.
 * Especially, LilyClients should release HBase connections they use, but at the
 * same not close HBase connections that don't belong to them.
 */
public class LilyClientHBaseConnectionsTest {
    @Test
    public void testZkConnectionsGoneAfterLilyClientStop() throws Exception {

        LilyClient lilyClient1 = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);
        LilyClient lilyClient2 = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);
        LilyClient lilyClient3 = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);

        assertEquals(3, HConnectionTestingUtility.getConnectionCount());

        Closer.close(lilyClient1);

        assertEquals(2, HConnectionTestingUtility.getConnectionCount());

        Closer.close(lilyClient2);

        assertEquals(1, HConnectionTestingUtility.getConnectionCount());

        Closer.close(lilyClient3);

        assertEquals(0, HConnectionTestingUtility.getConnectionCount());
    }
}
