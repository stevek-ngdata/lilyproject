/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rowlog.admin.cli;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogSubscription;

/**
 * Command-line command to show all known rowlogs.
 * 
 * <p>For each rowlog its properties are shown and a list of the registered subscriptions is shown.
 * <br>For each subscription its properties are shown and a list of the registered listeners.
 */
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
    }

}
