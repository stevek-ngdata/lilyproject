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
    public static void initMapperJob(RecordScan scan, String zooKeeperConnectString, Repository repository, Job job) {
        if (scan != null) {
            try {
                JsonNode node = RecordScanWriter.INSTANCE.toJson(scan, new WriteOptions(), repository);
                String scanData = JsonFormat.serializeAsString(node);
                job.getConfiguration().set(LilyInputFormat.SCAN, scanData);
            } catch (Exception e) {
                ExceptionUtil.handleInterrupt(e);
                throw new RuntimeException(e);
            }
        }
        job.getConfiguration().set(LilyInputFormat.ZK_CONNECT_STRING, zooKeeperConnectString);
    }
}
