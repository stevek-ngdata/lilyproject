package ${package};

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lilyproject.client.LilyClient;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.util.io.Closer;

import java.io.IOException;

/**
 * Sets up and launches a Lily-based MapReduce job.
 */
public class MyJob extends Configured implements Tool {
    private String zkConnectString;

    public static void main(String[] args) throws Exception {
        // Let <code>ToolRunner</code> handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        int result = parseArgs(args);
        if (result != 0) {
            return result;
        }

        Configuration config = getConf();

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
        LRepository repository = lilyClient.getDefaultRepository();

        // Utility method will configure everything related to LilyInputFormat
        LilyMapReduceUtil.initMapperJob(scan, zkConnectString, repository, job);

        Closer.close(lilyClient);

        // Launch the job
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error executing job!");
        }

        return 0;
    }

    @SuppressWarnings("static-access")
    protected int parseArgs(String[] args) {
        Options cliOptions = new Options();

        Option zkOption = OptionBuilder
                .isRequired()
                .withArgName("connection-string")
                .hasArg()
                .withDescription("ZooKeeper connection string: hostname1:port,hostname2:port,...")
                .withLongOpt("zookeeper")
                .create("z");
        cliOptions.addOption(zkOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.out.println();

            HelpFormatter help = new HelpFormatter();
            help.printHelp(getClass().getSimpleName(), cliOptions, true);
            return 1;
        }

        zkConnectString = cmd.getOptionValue(zkOption.getOpt());

        return 0;
    }
}
