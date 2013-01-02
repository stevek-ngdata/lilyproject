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

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;

/**
 * Command-line command to update the properties of a rowlog.
 * <p/>
 * <p>Only the properties of the rowlog can be updated this way, not the subscriptions
 * that are registered for the rowlog.
 * <p/>
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
        List<Option> options = super.getOptions();

        options.add(rowLogIdOption);
        options.add(respectOrderOption);
        options.add(notifyEnabledOption);
        options.add(notifyDelayOption);
        options.add(minimalProcessDelayOption);
        options.add(wakeupTimeoutOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

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
