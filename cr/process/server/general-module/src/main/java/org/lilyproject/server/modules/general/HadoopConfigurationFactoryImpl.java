/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.server.modules.general;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.kauriproject.conf.Conf;
import org.lilyproject.util.io.Closer;

public class HadoopConfigurationFactoryImpl implements HadoopConfigurationFactory {
    private Conf hbaseConf;
    private Conf mrConf;
    private String zkConnectString;
    private int zkSessionTimeout;

    private Configuration hbaseConfig;

    private Log log = LogFactory.getLog(getClass());

    public HadoopConfigurationFactoryImpl(Conf hbaseConf, Conf mrConf, String zkConnectString, int zkSessionTimeout)
            throws Exception {
        this.hbaseConf = hbaseConf;
        this.mrConf = mrConf;
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;

        waitOnHBase();
    }

    private void waitOnHBase() throws Exception {
        // This code serves to wait till HBase is ready. Note that both ZooKeeper and HBase have retry-loops too,
        // so these are even more retries. The purpose is that when all processes are started together (e.g. on system
        // startup) it can take a while for things to come up.
        Configuration conf = getHBaseConf();

        int attempt = 0;
        boolean connected = false;
        HTable table = null;
        while (attempt < 3) {
            try {
                table = new HTable(conf, HConstants.META_TABLE_NAME);
                connected = true;
                break;
            } catch (ZooKeeperConnectionException e) {
                log.warn("ZooKeeperConnectionException while trying to connect to HBase, attempt = " + attempt, e);
            } catch (RetriesExhaustedException e) {
                log.warn("RetriesExhaustedException while trying to connect to HBase, attempt = " + attempt, e);
            }
            attempt++;
        }

        if (!connected) {
            throw new Exception("Could not connect to HBase after several attempts, giving up. Check log for problems.");
        }

        long before = System.currentTimeMillis();
        ResultScanner s = table.getScanner(new Scan());
        while (s.next() != null) {
        }
        Closer.close(s);

        long duration = System.currentTimeMillis() - before;
        if (duration > 1000) {
            log.warn("Scanning the META table on Lily startup took " + duration + " ms.");
        }

    }

    public Configuration getHBaseConf() {
        // To enable reuse of HBase connections, we should always return the same Configuration instance
        if (hbaseConfig == null) {
            hbaseConfig = HBaseConfiguration.create();

            for (Conf conf : hbaseConf.getChild("properties").getChildren("property")) {
                String name = conf.getRequiredChild("name").getValue();
                String value = conf.getRequiredChild("value").getValue();
                hbaseConfig.set(name, value);
            }
        }

        return hbaseConfig;
    }

    public Configuration getMapReduceConf() {
        Configuration hadoopConf = new Configuration();

        for (Conf conf : mrConf.getChild("properties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        return hadoopConf;
    }

    public Configuration getMapReduceConf(Conf subConf) {
        Configuration hadoopConf = new Configuration();

        for (Conf conf : mrConf.getChild("properties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        for (Conf conf : subConf.getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        return hadoopConf;
    }

    public String getZooKeeperConnectString() {
        return zkConnectString;
    }

    public int getZooKeeperSessionTimeout() {
        return zkSessionTimeout;
    }
}
