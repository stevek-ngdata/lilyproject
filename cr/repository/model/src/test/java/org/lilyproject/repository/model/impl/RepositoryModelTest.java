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
package org.lilyproject.repository.model.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.model.api.RepositoryDefinition;
import org.lilyproject.repository.model.api.RepositoryModel;
import org.lilyproject.repository.model.api.RepositoryModelEvent;
import org.lilyproject.repository.model.api.RepositoryModelEventType;
import org.lilyproject.repository.model.api.RepositoryModelListener;
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

public class RepositoryModelTest {
    private MiniZooKeeperCluster zkCluster;
    private File zkDir;
    private int zkClientPort;
    private ZooKeeperItf zk;
    private RepositoryModel repositoryModel;

    @Before
    public void setUpBeforeClass() throws Exception {
        zkDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "lily.repositorymodeltest");
        zkClientPort = NetUtils.getFreePort();

        zkCluster = new MiniZooKeeperCluster();
        zkCluster.setDefaultClientPort(zkClientPort);
        zkCluster.startup(zkDir);

        zk = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        repositoryModel = new RepositoryModelImpl(zk);
    }

    @After
    public void tearDownAfterClass() throws Exception {
        if (zk != null) {
            zk.close();
        }

        if (repositoryModel != null) {
            ((RepositoryModelImpl)repositoryModel).close();
        }

        if (zkCluster != null) {
            zkCluster.shutdown();
            FileUtils.deleteDirectory(zkDir);
        }
    }

    @Test
    public void testRepositories() throws Exception {
        TestListener listener = new TestListener();
        repositoryModel.registerListener(listener);

        repositoryModel.create("repo1");
        listener.waitForEvents(1);
        listener.verifyEvents(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_ADDED, "repo1"));

        assertFalse(repositoryModel.repositoryExistsAndActive("repo1"));

        assertEquals(2, repositoryModel.getRepositories().size()); // 2 because the default repository is also there

        repositoryModel.updateRepository(new RepositoryDefinition("repo1", RepositoryDefinition.RepositoryLifecycleState.ACTIVE));
        listener.waitForEvents(1);
        listener.verifyEvents(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_UPDATED, "repo1"));

        assertTrue(repositoryModel.repositoryExistsAndActive("repo1"));

        repositoryModel.delete("repo1");
        listener.waitForEvents(1);
        listener.verifyEvents(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_UPDATED, "repo1"));

        assertEquals(2, repositoryModel.getRepositories().size());
        assertEquals(RepositoryDefinition.RepositoryLifecycleState.DELETE_REQUESTED, repositoryModel.getRepository("repo1").getLifecycleState());

        repositoryModel.deleteDirect("repo1");
        assertEquals(1, repositoryModel.getRepositories().size());
        listener.waitForEvents(1);
        listener.verifyEvents(new RepositoryModelEvent(RepositoryModelEventType.REPOSITORY_REMOVED, "repo1"));

        repositoryModel.unregisterListener(listener);
        repositoryModel.create("repo1");
        Thread.sleep(20); // this is not very robust, but if the code would be wrong this will fail most of the time
        listener.waitForEvents(0);
    }

    @Test
    public void testTwoModelInstances() throws Exception {
        ZooKeeperItf zk1 = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        ZooKeeperItf zk2 = ZkUtil.connect("localhost:" + zkClientPort, 5000);
        RepositoryModel repositoryModel1 = new RepositoryModelImpl(zk1);
        RepositoryModel repositoryModel2 = new RepositoryModelImpl(zk2);

        TestListener model1Listener = new TestListener();
        assertEquals(1, repositoryModel1.getRepositories(model1Listener).size());

        TestListener model2Listener = new TestListener();
        assertEquals(1, repositoryModel2.getRepositories(model2Listener).size());

        repositoryModel1.create("tenant1");
        repositoryModel1.create("tenant2");
        repositoryModel2.create("tenant3");
        repositoryModel2.create("tenant4");

        model2Listener.waitForEvents(4);
        model1Listener.waitForEvents(4);

        assertEquals(5, repositoryModel1.getRepositories().size());
        assertEquals(5, repositoryModel2.getRepositories().size());

        ((RepositoryModelImpl)repositoryModel1).close();
        ((RepositoryModelImpl)repositoryModel2).close();

        zk1.close();
        zk2.close();
    }

    @Test(expected = Exception.class)
    public void testDefaultRepositoryCannotBeDeleted() throws Exception {
        repositoryModel.delete("default");
    }

    @Test(expected = Exception.class)
    public void testDefaultRepositoryCannotBeDirectDeleted() throws Exception {
        repositoryModel.deleteDirect("default");
    }

    @Test(expected = Exception.class)
    public void testDefaultRepositoryCannotBeUpdated() throws Exception {
        repositoryModel.updateRepository(new RepositoryDefinition("default", RepositoryDefinition.RepositoryLifecycleState.ACTIVE));
    }

    @Test(expected = Exception.class)
    public void testInvalidRepositoryName() throws Exception {
        repositoryModel.create("repository#");
    }

    @Test(expected = Exception.class)
    public void testInvalidRepositoryName2() throws Exception {
        repositoryModel.create("repository__");
    }

    public static class TestListener implements RepositoryModelListener {
        private List<RepositoryModelEvent> events = new ArrayList<RepositoryModelEvent>();

        @Override
        public void process(RepositoryModelEvent event) {
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

        public void verifyEvents(RepositoryModelEvent... expectedEvents) {
            if (events.size() != expectedEvents.length) {
                if (events.size() > 0) {
                    System.out.println("The events are:");
                    for (RepositoryModelEvent event : events) {
                        System.out.println(event.getEventType() + " - " + event.getRepositoryName());
                    }
                } else {
                    System.out.println("There are no events.");
                }

                assertEquals("Expected number of events", expectedEvents.length, events.size());
            }

            Set<RepositoryModelEvent> expectedEventsSet = new HashSet<RepositoryModelEvent>(Arrays.asList(expectedEvents));

            for (RepositoryModelEvent event : expectedEvents) {
                if (!events.contains(event)) {
                    fail("Expected event not present among events: " + event);
                }
            }

            for (RepositoryModelEvent event : events) {
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
