package org.lilyproject.rowlog.admin.cli;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.lilyproject.cli.BaseZkCliTool;
import org.lilyproject.rowlog.impl.RowLogConfigurationManagerImpl;
import org.lilyproject.util.zookeeper.StateWatchingZooKeeper;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public abstract class BaseRowLogAdminCli extends BaseZkCliTool {
    
    private Option rowLogIdOption;
    private String rowLogId;
    protected RowLogConfigurationManagerImpl rowLogConfigurationManager;

    public BaseRowLogAdminCli() {
        // Here we instantiate various options, but it is up to subclasses to decide which ones
        // they acutally want to use (see getOptions() method).

        rowLogIdOption = OptionBuilder
                .withArgName("rowlogid")
                .hasArg()
                .withDescription("RowLog id.")
                .withLongOpt("rowlogid")
                .create("r");
    }
    
    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        return options;
    }
    
    @Override
    protected int processOptions(CommandLine cmd) throws Exception {
        int result = super.processOptions(cmd);
        if (result != 0)
            return result;

        if (cmd.hasOption(rowLogIdOption.getOpt())) {
            rowLogId = cmd.getOptionValue(rowLogIdOption.getOpt());
        }
        
        return 0;
    }
    
    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0)
            return result;
        
        final ZooKeeperItf zk = new StateWatchingZooKeeper(zkConnectionString, 10000);
        
        rowLogConfigurationManager = new RowLogConfigurationManagerImpl(zk);
        
        return 0;
    }
    
}
