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
package org.lilyproject.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.tools.import_.json.RecordScanWriter;
import org.lilyproject.tools.import_.json.WriteOptions;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.json.JsonFormat;

public class LilyMapReduceUtil {
    public static final String ZK_CONNECT_STRING = "lily.mapreduce.zookeeper";

    /**
     * Set the necessary parameters inside the job configuration for using Lily as input.
     */
    public static void initMapperJob(RecordScan scan, String zooKeeperConnectString, Repository repository, Job job) {
        initMapperJob(scan, false, zooKeeperConnectString, repository, job);
    }
    
    /**
     * Set the necessary parameters inside the job configuration for using Lily as input.
     */
    public static void initMapperJob(RecordScan scan, boolean returnIdRecords, String zooKeeperConnectString, Repository repository, Job job) {
        if (returnIdRecords)
            job.setInputFormatClass(LilyIdScanInputFormat.class);
        else 
            job.setInputFormatClass(LilyScanInputFormat.class);
        
        job.getConfiguration().set(ZK_CONNECT_STRING, zooKeeperConnectString);

        if (scan != null) {
            try {
                JsonNode node = RecordScanWriter.INSTANCE.toJson(scan, new WriteOptions(), repository);
                String scanData = JsonFormat.serializeAsString(node);
                job.getConfiguration().set(AbstractLilyScanInputFormat.SCAN, scanData);
            } catch (Exception e) {
                ExceptionUtil.handleInterrupt(e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates a LilyClient based on the information found in the Configuration object.
     */
    public static LilyClient getLilyClient(Configuration conf) throws InterruptedException {
        String zkConnectString = conf.get(ZK_CONNECT_STRING);
        try {
            return new LilyClient(zkConnectString, 30000);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
