package ${package};

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.lilyproject.client.LilyClient;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.util.io.Closer;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * Sets up & launches a Lily-based MapReduce job.
 */
public class MyJob {
    public static void main(String[] args) throws Exception {
        String masterHost;
        if (args.length == 0) {
            System.out.println();
            System.out.println("Assuming your JobTracker, Namenode and ZooKeeper runs on localhost.");
            System.out.println();
            masterHost = "localhost";
        } else {
            masterHost = args[0];
        }
        
        Configuration config = HBaseConfiguration.create();
        // If you launch this using hadoop tools and your -site.xml files are configured, then
        // you can drop these settings here.
        config.set("mapred.job.tracker", masterHost + ":9001");
        config.set("fs.default.name", "hdfs://" + masterHost + ":8020");
        String zooKeeperHost = masterHost;

        Job job = new Job(config, "MyJob");
        job.setJarByClass(MyJob.class);

        //
        // Set up the map part
        //
        job.setMapperClass(MyMapper.class);

        // If your map has no output, you would set OutputFormatClass to NullOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path("my-job-output"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // TODO lame we need LilyClient here just to seriliaze the RecordScan
        LilyClient lilyClient = new LilyClient(zooKeeperHost, 30000);
        Repository repository = lilyClient.getRepository();

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(new QName("mrsample", "Document")));
        LilyMapReduceUtil.initMapperJob(scan, zooKeeperHost, repository, job);

        Closer.close(lilyClient);

        //
        // Set up the reducer part
        //

        // If you have no reducer, just leave out this part
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);

        //
        // Launch the job
        //
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error executing job!");
        }
    }
}
