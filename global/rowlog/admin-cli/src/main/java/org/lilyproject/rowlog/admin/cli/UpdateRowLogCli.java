package org.lilyproject.rowlog.admin.cli;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;

public class UpdateRowLogCli extends BaseRowLogAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-update-rowlog";
    }

    public static void main(String[] args) throws InterruptedException {
        new UpdateRowLogCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options =  super.getOptions();
        
        rowLogIdOption.setRequired(true);

        options.add(rowLogIdOption);
        options.add(respectOrderOption);
        options.add(notifyEnabledOption);
        options.add(notifyDelayOption);
        options.add(minimalProcessDelayOption);
        options.add(wakeupTimeoutOption);

        return options;
    }
    
    @Override
    public int run(CommandLine cmd) throws Exception  {
        try {
            int result = super.run(cmd);
            if (result != 0)
                return result;
            RowLogConfig rowLogConfig = rowLogConfigurationManager.getRowLogs().get(rowLogId);
            if (rowLogConfig == null) {
                System.out.println("Rowlog does not exist: " + rowLogId);
                return 1;
            }
            if (respectOrder != null) {
                rowLogConfig.setRespectOrder(respectOrder);
            }
            if (notifyEnabled != null) {
                rowLogConfig.setEnableNotify(notifyEnabled);
            }
            if (notifyDelay != null) {
                rowLogConfig.setNotifyDelay(notifyDelay);
            }
            if (minimalProcessDelay != null) {
                rowLogConfig.setMinimalProcessDelay(minimalProcessDelay);
            }
            if (wakeupTimeout != null) {
                rowLogConfig.setWakeupTimeout(wakeupTimeout);
            }
            rowLogConfigurationManager.updateRowLog(rowLogId, rowLogConfig);
            return 0;
        } finally {
            if (rowLogConfigurationManager != null)
                rowLogConfigurationManager.shutdown();
        }
    }
}
