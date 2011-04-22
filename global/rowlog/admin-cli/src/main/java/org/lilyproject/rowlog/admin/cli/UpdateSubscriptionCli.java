package org.lilyproject.rowlog.admin.cli;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogSubscription;

/**
 * Command-line command to update the properties of a subscription.
 * 
 * <p>Only the properties of the subscription can be updated this way, not the listeners
 * that are registered to the subscritpion.
 * 
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
        List<Option> options =  super.getOptions();
        
        rowLogIdOption.setRequired(true);
        subscriptionIdOption.setRequired(true);
        
        options.add(rowLogIdOption);
        options.add(subscriptionIdOption);
        options.add(subscriptionTypeOption);
        options.add(subscriptionOrderNrOption);

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
        } finally {
            if (rowLogConfigurationManager != null)
                rowLogConfigurationManager.shutdown();
        }
    }
}
