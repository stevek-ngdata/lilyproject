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
package org.lilyproject.lilyservertestfw.integration;

import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.tools.import_.cli.JsonImport;

import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

public class ResetLilyStateTest {
    /**
     * The purpose of this test is to check that after resetLilyState, and when continuing to use
     * the same LilyClient instance, the schema cache in the local client is emptied.
     */
    @Test
    public void testSchemaCacheEmptyAfterResetLilyState() throws Exception {
        LilyClient lilyClient = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);
        Repository repository = lilyClient.getRepository();


        for (int i = 0; i < 2; i++) {
            resetLilyState();

            // Just call a dummy method to wait for the repository to become available
            repository.newRecord();

            // Give caches some time to notice the changes going on
            Thread.sleep(2000);

            // Check that the field type and record type caches are empty
            assertEquals(0, repository.getTypeManager().getRecordTypes().size());
            assertEquals(0, repository.getTypeManager().getRecordTypesWithoutCache().size());

            // There's always the last vtag field type defined
            assertEquals(1, repository.getTypeManager().getFieldTypes().size());
            assertEquals(1, repository.getTypeManager().getFieldTypesWithoutCache().size());

            // Load a schema
            InputStream is = ResetLilyStateTest.class.getResourceAsStream("schema.json");
            JsonImport.load(repository, is, false);
            is.close();

            // Create a record to assure the schema we just created is in the cache.
            // If the cache would not have been cleared correctly (which is ruled out already
            // by the above assertions), then record creation might fail because it uses the
            // schema IDs of the types from the previous run.
            repository.recordBuilder()
                    .defaultNamespace("com.mycompany")
                    .recordType("Type1")
                    .field("field1", "foo")
                    .create();
        }

        lilyClient.close();
    }

    private void resetLilyState() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost:10102/jndi/rmi://localhost:10102/jmxrmi");

        JMXConnector connector = JMXConnectorFactory.connect(url);
        connector.connect();

        ObjectName lilyLauncher = new ObjectName("LilyLauncher:name=Launcher");
        connector.getMBeanServerConnection().invoke(lilyLauncher, "resetLilyState", new Object[0], new String[0]);
        connector.close();
    }

    /**
     * The purpose of this test is to check that the schema cache keeps refreshing after resetLilyState
     * has been called, and existing LilyClient instances are continued to be used.
     * This would fail if the schema cache wouldn't detect that all paths were erased from ZooKeeper, and
     * that it hence has to install new ZooKeeper watchers.
     */
    @Test
    public void testSchemaCacheRefreshingAfterResetLilyState() throws Exception {
        // Create two LilyClient's: each will have its own schema cache
        LilyClient lilyClient1 = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);
        Repository repository1 = lilyClient1.getRepository();

        LilyClient lilyClient2 = new LilyClient(System.getProperty("zkConn", "localhost:2181"), 20000);
        Repository repository2 = lilyClient2.getRepository();

        resetLilyState();

        // After resetLilyState, there should be no types
        assertEquals(0, repository2.getTypeManager().getRecordTypes().size());

        // Create schema via client 1
        InputStream is = ResetLilyStateTest.class.getResourceAsStream("schema.json");
        JsonImport.load(repository1, is, false);
        is.close();

        // Give client 2 just a bit of time to refresh its cache
        Thread.sleep(2000);

        // Check client 2 knows the type know
        assertEquals(1, repository2.getTypeManager().getRecordTypes().size());

        lilyClient1.close();
        lilyClient2.close();
    }
}
