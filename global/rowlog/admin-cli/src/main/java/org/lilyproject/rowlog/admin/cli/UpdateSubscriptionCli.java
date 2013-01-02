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
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogSubscription;

/**
 * Command-line command to update the properties of a subscription.
 * <p/>
 * <p>Only the properties of the subscription can be updated this way, not the listeners
 * that are registered to the subscritpion.
 * <p/>
 * <p>The properties that can be changed are:
 * <br>- subscription type : the type of the subscription VM (in-vm) or Netty (remote)
 * <br>- order nr : the order nr of the subscription wrt the other subscriptions of the same rowlog
 */
public class UpdateSubscriptionCli extends BaseRowLogAdminCli {
    @Override
    protected String getCmdName() {
        return "lily-update-subscription";
    }

    public static void main(String[] args) throws InterruptedException {
        new UpdateSubscriptionCli().start(args);
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        options.add(rowLogIdOption);
        options.add(subscriptionIdOption);
        options.add(subscriptionTypeOption);
        options.add(subscriptionOrderNrOption);

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

        if (subscriptionId == null) {
            System.out.println("Specify rowlog ID with -" + subscriptionIdOption.getOpt());
            return 1;
        }

        RowLogConfig rowLogConfig = rowLogConfigurationManager.getRowLogs().get(rowLogId);
        if (rowLogConfig == null) {
            System.out.println("Rowlog '" + rowLogId + "'does not exist: ");
            return 1;
        }
        RowLogSubscription subscription = null;
        List<RowLogSubscription> subscriptions = rowLogConfigurationManager.getSubscriptions(rowLogId);
        for (RowLogSubscription rowLogSubscription : subscriptions) {
            if (rowLogSubscription.getId().equals(subscriptionId)) {
                subscription = rowLogSubscription;
                break;
            }
        }
        if (subscription == null) {
            System.out.println("Subscription '" + subscriptionId + "' does not exist for rowlog '" + rowLogId + "'");
            return 1;
        }
        if (type == null) {
            type = subscription.getType();
        }
        if (orderNr == null) {
            orderNr = subscription.getOrderNr();
        }
        try {
            rowLogConfigurationManager.updateSubscription(rowLogId, subscriptionId, type, orderNr);
        } catch (RowLogException e) {
            System.out.println("Subscription '" + subscriptionId + "' does not exist for rowlog '" + rowLogId + "'");
            return 1;
        }
        return 0;
    }
}
