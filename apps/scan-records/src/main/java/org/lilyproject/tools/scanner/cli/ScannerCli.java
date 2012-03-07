package org.lilyproject.tools.scanner.cli;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.client.LilyClient;
import org.lilyproject.util.io.Closer;

public class ScannerCli extends BaseZkCliTool {
    private LilyClient lilyClient;
    private Option limitOption;
    private Option countOption;
    private Option printOption;
    private Option configOption;
    private Option startOption;
    private Option stopOption;

    /**
     * @param args
     */
    public static void main(String[] args) {
        new ScannerCli().start(args);

    }

    @Override
    protected String getCmdName() {
        return "lily-scan-records";
    }

    @Override
    protected String getVersion() {
        return readVersion("org.lilyproject", "lily-scan-records");
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        limitOption = OptionBuilder
                .withArgName("number")
                .hasArg()
                .withDescription("Limit printing to a number of records")
                .withLongOpt("limit")
                .create("l");
        countOption = OptionBuilder
                .withDescription("Count the number of records")
                .withLongOpt("count")
                .create("c");
        printOption = OptionBuilder
                .withDescription("Print records to the command line")
                .withLongOpt("print")
                .create("p");
        configOption = OptionBuilder
                .hasArg()
                .withArgName("file")
                .withDescription("Configure the record scanner using a json file")
                .withLongOpt("config")
                .create();
        startOption = OptionBuilder
                .hasArg()
                .withArgName("id")
                .withDescription("Scan records starting at the record with the given ID")
                .withLongOpt("start")
                .create();
        stopOption = OptionBuilder
                .hasArg()
                .withArgName("id")
                .withDescription("Scan records stopping at the record with the given ID")
                .withLongOpt("stop")
                .create();

        options.add(printOption);
        options.add(limitOption);
        options.add(countOption);
        options.add(configOption);
        options.add(startOption);
        options.add(stopOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;

        System.out.println("This is the lily record scanner.");

        if (!cmd.hasOption(printOption.getOpt()) && !cmd.hasOption(countOption.getOpt())) {
            printHelp();
            return 0;
        }

        lilyClient = new LilyClient(zkConnectionString, zkSessionTimeout);
        String startId = cmd.hasOption(startOption.getLongOpt()) ? cmd.getOptionValue(startOption.getLongOpt()) : null;
        String stopId = cmd.hasOption(stopOption.getLongOpt()) ? cmd.getOptionValue(stopOption.getLongOpt()) : null;
        File configFile = cmd.hasOption(configOption.getLongOpt()) ? new File (cmd.getOptionValue(configOption.getLongOpt())) : null;
        long limit = cmd.hasOption(limitOption.getLongOpt()) ? Long.parseLong(cmd.getOptionValue(limitOption.getLongOpt())) : -1;
        
  
        
        if (cmd.hasOption(countOption.getOpt())) {
            
            RecordScanTool.count(lilyClient.getRepository(), startId, stopId, configFile);
        }
        if (cmd.hasOption(printOption.getOpt())) {
            RecordScanTool.print(lilyClient.getRepository(), startId, stopId, limit, configFile);
        }

        return 0;
    }

    @Override
    protected void cleanup() {
        Closer.close(lilyClient);
        super.cleanup();
    }

}
