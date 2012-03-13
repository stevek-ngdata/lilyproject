package org.lilyproject.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.tools.import_.json.RecordScanWriter;
import org.lilyproject.tools.import_.json.WriteOptions;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.json.JsonFormat;

public class LilyMapReduceUtil {
    public static final String ZK_CONNECT_STRING = "lily.mapreduce.zookeeper";

    /**
     * Set the necessary parameters inside the job configuration for a Lily based
     * MapReduce job.
     */
    public static void initMapperJob(RecordScan scan, String zooKeeperConnectString, Repository repository, Job job) {
        job.setInputFormatClass(LilyScanInputFormat.class);
        job.getConfiguration().set(LilyScanInputFormat.ZK_CONNECT_STRING, zooKeeperConnectString);

        job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
        job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");

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
}
