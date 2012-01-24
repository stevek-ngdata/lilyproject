package org.lilyproject.rowlog.admin.cli;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;

/**
 * Command-line command to update the properties of a rowlog.
 * 
 * <p>Only the properties of the rowlog can be updated this way, not the subscriptions
 * that are registered for the rowlog.
 * 
 * <p>The properties that can be changed are:
 * <br>- respect order : if the order of the subscriptions needs to be respected
 * <br>- notify enabled : if the processor needs to be notified when a new message is put on the rowlog
 * <br>- notify delay : the minimal time between notifications sent to the rowlog
 * <br>- minimal process delay : the minimal age a message should have before the processor will process it
 * <br>- wakeup timeout : the time the processor waits (in case no notification was received) 
 * before checking if there are new messages available
 */
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
        int result = super.run(cmd);
        if (result != 0)
            return result;

        if (rowLogId == null) {
            System.out.println("Specify rowlog ID with -" + rowLogIdOption.getOpt());
            return 1;
        }

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
    }
}
