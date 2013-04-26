/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.tenant.model.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.tenant.model.api.Tenant;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.tenant.model.api.TenantModelEvent;
import org.lilyproject.tenant.model.api.TenantModelEventType;
import org.lilyproject.tenant.model.api.TenantModelListener;
import org.lilyproject.util.net.NetUtils;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.lilyproject.tenant.model.api.Tenant.TenantLifecycleState;

public class TenantModelTest {
    private MiniZooKeeperCluster zkCluster;
    private File zkDir;
    private int zkClientPort;
    private ZooKeeperItf zk;
    private TenantModel tenantModel;

    @Before
    public void setUpBeforeClass() throws Exception {
        zkDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "lily.tenantmodeltest");
        zkClientPort = NetUtils.getFreePort();

        zkCluster = new MiniZooKeeperCluster();
        zkCluster.setDefaultClientPort(zkClientPort);
        zkCluster.startup(zkDir);

        zk = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        tenantModel = new TenantModelImpl(zk);
    }

    @After
    public void tearDownAfterClass() throws Exception {
        if (zk != null) {
            zk.close();
        }

        if (tenantModel != null) {
            ((TenantModelImpl)tenantModel).close();
        }

        if (zkCluster != null) {
            zkCluster.shutdown();
            FileUtils.deleteDirectory(zkDir);
        }
    }

    @Test
    public void testTenants() throws Exception {
        TestListener listener = new TestListener();
        tenantModel.registerListener(listener);

        tenantModel.create("tenant1");
        listener.waitForEvents(1);
        listener.verifyEvents(new TenantModelEvent(TenantModelEventType.TENANT_ADDED, "tenant1"));

        assertFalse(tenantModel.tenantExistsAndActive("tenant1"));

        assertEquals(2, tenantModel.getTenants().size()); // 2 because the public tenant is also there

        tenantModel.updateTenant(new Tenant("tenant1", TenantLifecycleState.ACTIVE));
        listener.waitForEvents(1);
        listener.verifyEvents(new TenantModelEvent(TenantModelEventType.TENANT_UPDATED, "tenant1"));

        assertTrue(tenantModel.tenantExistsAndActive("tenant1"));

        tenantModel.delete("tenant1");
        listener.waitForEvents(1);
        listener.verifyEvents(new TenantModelEvent(TenantModelEventType.TENANT_UPDATED, "tenant1"));

        assertEquals(2, tenantModel.getTenants().size());
        assertEquals(TenantLifecycleState.DELETE_REQUESTED, tenantModel.getTenant("tenant1").getLifecycleState());

        tenantModel.deleteDirect("tenant1");
        assertEquals(1, tenantModel.getTenants().size());
        listener.waitForEvents(1);
        listener.verifyEvents(new TenantModelEvent(TenantModelEventType.TENANT_REMOVED, "tenant1"));

        tenantModel.unregisterListener(listener);
        tenantModel.create("tenant1");
        Thread.sleep(20); // this is not very robust, but if the code would be wrong this will fail most of the time
        listener.waitForEvents(0);
    }

    @Test
    public void testTwoModelInstances() throws Exception {
        ZooKeeperItf zk1 = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        ZooKeeperItf zk2 = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        TenantModel tenantModel1 = new TenantModelImpl(zk1);
        TenantModel tenantModel2 = new TenantModelImpl(zk2);

        TestListener model1Listener = new TestListener();
        assertEquals(1, tenantModel1.getTenants(model1Listener).size());

        TestListener model2Listener = new TestListener();
        assertEquals(1, tenantModel2.getTenants(model2Listener).size());

        tenantModel1.create("tenant1");
        tenantModel1.create("tenant2");
        tenantModel2.create("tenant3");
        tenantModel2.create("tenant4");

        model2Listener.waitForEvents(4);
        model1Listener.waitForEvents(4);

        assertEquals(5, tenantModel1.getTenants().size());
        assertEquals(5, tenantModel2.getTenants().size());

        ((TenantModelImpl)tenantModel1).close();
        ((TenantModelImpl)tenantModel2).close();

        zk1.close();
        zk2.close();
    }

    @Test(expected = Exception.class)
    public void testPublicTenantCannotBeDeleted() throws Exception {
        tenantModel.delete("public");
    }

    @Test(expected = Exception.class)
    public void testPublicTenantCannotBeDirectDeleted() throws Exception {
        tenantModel.deleteDirect("public");
    }

    @Test(expected = Exception.class)
    public void testPublicTenantCannotBeUpdated() throws Exception {
        tenantModel.updateTenant(new Tenant("public", TenantLifecycleState.ACTIVE));
    }

    @Test(expected = Exception.class)
    public void testInvalidTenantName() throws Exception {
        tenantModel.create("tenant#");
    }

    @Test(expected = Exception.class)
    public void testInvalidTenantName2() throws Exception {
        tenantModel.create("tenant__");
    }

    public static class TestListener implements TenantModelListener {
        private List<TenantModelEvent> events = new ArrayList<TenantModelEvent>();

        @Override
        public void process(TenantModelEvent event) {
            events.add(event);
        }

        public void waitForEvents(int count) throws InterruptedException {
            long now = System.currentTimeMillis();
            synchronized (this) {
                while (events.size() < count && System.currentTimeMillis() - now < 60000) {
                    Thread.sleep(50);
                }
            }
        }

        public void verifyEvents(TenantModelEvent... expectedEvents) {
            if (events.size() != expectedEvents.length) {
                if (events.size() > 0) {
                    System.out.println("The events are:");
                    for (TenantModelEvent event : events) {
                        System.out.println(event.getEventType() + " - " + event.getTenantName());
                    }
                } else {
                    System.out.println("There are no events.");
                }

                assertEquals("Expected number of events", expectedEvents.length, events.size());
            }

            Set<TenantModelEvent> expectedEventsSet = new HashSet<TenantModelEvent>(Arrays.asList(expectedEvents));

            for (TenantModelEvent event : expectedEvents) {
                if (!events.contains(event)) {
                    fail("Expected event not present among events: " + event);
                }
            }

            for (TenantModelEvent event : events) {
                if (!expectedEventsSet.contains(event)) {
                    fail("Got an event which is not among the expected events: " + event);
                }
            }

            events.clear();
        }
    }

    public static class DummyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent watchedEvent) {
        }
    }
}
