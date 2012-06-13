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
package org.lilyproject.hadooptestfw;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.hadooptestfw.fork.HBaseTestingUtility;

import java.io.File;
import java.io.IOException;

public class HBaseTestingUtilityFactory {
    public static final String TEST_DIR_KEY = "lily.hbasetestingutility.dir";

    /**
     * Creates an HBaseTestingUtility with settings applied such that everything will be stored below the
     * supplied directory and makes (to some extent) use of standard port numbers.
     *
     * @param conf HBase conf to use, as created by HBaseConfiguration.create().
     * @param tmpDir directory under which data of dfs, zookeeper, mr, ... will be stored
     * @param clearData can data be cleared (at startup or shutdown), use true unless you need the data from a previous
     *                  run
     */
    public static HBaseTestingUtility create(Configuration conf, File tmpDir, boolean clearData) throws IOException {

        // This location will be used for dfs, zookeeper, ...
        conf.set(TEST_DIR_KEY, createSubDir(tmpDir, "hbase-test-util"));

        // This property is picked up by our fork of MiniMRCluster (the default implementation was hardcoded
        // to use build/test/mapred/local)
        System.setProperty("mapred.local.dir", createSubDir(tmpDir, "mapred-local"));
        
        conf.set("mapred.local.dir", createSubDir(tmpDir, "mapred-local"));

        // Properties used for MiniMRCluster
        conf.set("hadoop.log.dir", createSubDir(tmpDir, "hadoop-logs"));
        conf.set("hadoop.tmp.dir", createSubDir(tmpDir, "mapred-output"));
        
        conf.set("mapred.system.dir", "/tmp/hadoop/mapred/system");
        conf.set("mapreduce.jobtracker.staging.root.dir", "/tmp/hadoop/mapred/staging");
        
        // Force default port numbers
        conf.set("hbase.master.info.port", "60010");
        conf.set("hbase.regionserver.info.port", "60030");

        // Allow more clients to connect concurrently (HBase default is 10)
        conf.set("hbase.regionserver.handler.count", "30");

        // Allow more clients to connect concurrently to hdfs (default is 3)
        conf.set("dfs.datanode.handler.count", "6");

        // Generic performance related settings
        conf.set("io.file.buffer.size", "65536");
        conf.set("hbase.hregion.memstore.flush.size", "268435456");

        // Disable the automatic closing of Hadoop FileSystem objects by its shutdown hook.
        // Otherwise, when stopping 'launch-test-lily' (LilyLauncher), the shutdown hook closes the filesystem
        // before HBase had the opportunity to flush its data. This then leads to (possibly long) recoveries
        // on the next startup (and even then, I've seen data loss, maybe sync is not active for the mini cluster?).
        conf.set("fs.automatic.close", "false");

        return new HBaseTestingUtility(conf, clearData);
    }

    private static String createSubDir(File parent, String child) throws IOException {
        File dir = new File(parent, child);
        FileUtils.forceMkdir(dir);
        return dir.getAbsolutePath();
    }
}
