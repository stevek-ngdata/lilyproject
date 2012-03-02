package org.lilyproject.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.tools.import_.json.RecordScanWriter;

public class LilyMapReduceUtil {
    public static void initMapperJob(RecordScan scan, String zooKeeperConnectString, Repository repository, Job job) {
        if (scan != null) {
            String scanData = RecordScanWriter.INSTANCE.toJsonString(scan, repository);
            job.getConfiguration().set(LilyInputFormat.SCAN, scanData);
        }
        job.getConfiguration().set(LilyInputFormat.ZK_CONNECT_STRING, zooKeeperConnectString);
    }
}
