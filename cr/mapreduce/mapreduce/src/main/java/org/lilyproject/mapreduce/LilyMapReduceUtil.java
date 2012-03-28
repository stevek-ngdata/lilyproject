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
        job.setInputFormatClass(LilyScanInputFormat.class);
        job.getConfiguration().set(ZK_CONNECT_STRING, zooKeeperConnectString);

        if (scan != null) {
            try {
                JsonNode node = RecordScanWriter.INSTANCE.toJson(scan, new WriteOptions(), repository);
                String scanData = JsonFormat.serializeAsString(node);
                job.getConfiguration().set(LilyScanInputFormat.SCAN, scanData);
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
