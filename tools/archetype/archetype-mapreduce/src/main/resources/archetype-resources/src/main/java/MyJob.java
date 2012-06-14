package ${package};

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.lilyproject.client.LilyClient;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.util.io.Closer;

import java.io.IOException;

/**
 * Sets up and launches a Lily-based MapReduce job.
 */
public class MyJob {
    public static void main(String[] args) throws Exception {
        String masterHost;
        if (args.length == 0) {
            System.out.println();
            System.out.println("Assuming your JobTracker, Namenode and ZooKeeper run on localhost.");
            System.out.println();
            masterHost = "localhost";
        } else {
            masterHost = args[0];
        }

        Configuration config = HBaseConfiguration.create();

        // If you launch this using Hadoop tools and your -site.xml files are configured, then
        // you can drop these settings here.
        config.set("mapred.job.tracker", masterHost + ":9001");
        config.set("fs.defaultFS", "hdfs://" + masterHost + ":8020");

        String zkConnectString = masterHost;

        Job job = new Job(config, "MyJob");
        job.setJarByClass(MyJob.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // The reducer writes directly to Lily, so for Hadoop there is no output to produce
        job.setOutputFormatClass(NullOutputFormat.class);

        // The RecordScan defines what subset of the records will be offered as input
        // to the map task.
        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(new QName("mrsample", "Document")));

        // Need LilyClient here just to be able to serialize the RecordScan.
        // This is a bit lame, will improve in the future.
        LilyClient lilyClient = new LilyClient(zkConnectString, 30000);
        Repository repository = lilyClient.getRepository();

        // Utility method will configure everything related to LilyInputFormat
        LilyMapReduceUtil.initMapperJob(scan, zkConnectString, repository, job);

        Closer.close(lilyClient);

        // Launch the job
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error executing job!");
        }
    }
}
