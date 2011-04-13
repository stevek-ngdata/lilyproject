package org.lilyproject.rowlog.admin.cli;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogSubscription;

public class ListRowLogsCli extends BaseRowLogAdminCli {

    @Override
    protected String getCmdName() {
        return "lily-list-rowlogs";
    }

    public static void main(String[] args) throws InterruptedException {
        new ListRowLogsCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        return super.getOptions();
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        try {
            int result = super.run(cmd);
            if (result != 0)
                return result;

            Map<String, RowLogConfig> rowLogs = rowLogConfigurationManager.getRowLogs();
            System.out.println("Number of rowlogs: " + rowLogs.size());

            for (Entry<String, RowLogConfig> rowLog : rowLogs.entrySet()) {
                System.out.println(rowLog.getKey());
                RowLogConfig rowLogConfig = rowLog.getValue();
                System.out.println("  + Respect order: " + rowLogConfig.isRespectOrder());
                System.out.println("  + Notify enabled: " + rowLogConfig.isEnableNotify());
                System.out.println("  + Notify delay: " + rowLogConfig.getNotifyDelay());
                System.out.println("  + Minimal process delay: " + rowLogConfig.getMinimalProcessDelay());
                System.out.println("  + Wakeup timeout: " + rowLogConfig.getWakeupTimeout());
                System.out.println();
                List<RowLogSubscription> subscriptions = rowLogConfigurationManager.getSubscriptions(rowLog.getKey());
                Collections.sort(subscriptions, new Comparator<RowLogSubscription>() {
                    @Override
                    public int compare(RowLogSubscription o1, RowLogSubscription o2) {
                        return new Integer(o1.getOrderNr()).compareTo(new Integer(o2.getOrderNr()));
                    }
                });
                System.out.println("  Number of subscriptions: " + subscriptions.size());
                for (RowLogSubscription subscription : subscriptions) {
                    System.out.println("  " + subscription.getId());
                    System.out.println("    + Type: " + subscription.getType().name());
                    System.out.println("    + Order nr: " + subscription.getOrderNr());
                    System.out.println();
                    List<String> listeners = rowLogConfigurationManager.getListeners(rowLog.getKey(), subscription
                            .getId());
                    System.out.println("    Number of listeners: " + listeners.size());
                    for (String listener : listeners) {
                        System.out.println("      " + listener);
                    }
                    System.out.println();
                }
            }

            return 0;
        } finally {
            if (rowLogConfigurationManager != null)
                rowLogConfigurationManager.shutdown();
        }
    }

}
