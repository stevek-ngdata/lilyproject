package org.lilyproject.tools.recordrowvisualizer;

public class ExecutionData {
    protected String subscriptionId;
    protected boolean success;
    protected String lock;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getLock() {
        return lock;
    }
}
