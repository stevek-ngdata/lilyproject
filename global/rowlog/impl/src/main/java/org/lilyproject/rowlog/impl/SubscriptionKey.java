package org.lilyproject.rowlog.impl;

public class SubscriptionKey {
    private String rowLogId;
    private String subscriptionId;

    public SubscriptionKey(String rowLogId, String subscriptionId) {
        this.rowLogId = rowLogId;
        this.subscriptionId = subscriptionId;
    }

    public String getRowLogId() {
        return rowLogId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubscriptionKey that = (SubscriptionKey)o;

        if (rowLogId != null ? !rowLogId.equals(that.rowLogId) : that.rowLogId != null) {
            return false;
        }

        if (subscriptionId != null ? !subscriptionId.equals(that.subscriptionId) : that.subscriptionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = rowLogId != null ? rowLogId.hashCode() : 0;
        result = 31 * result + (subscriptionId != null ? subscriptionId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[" +
                "rowLogId='" + rowLogId + '\'' +
                ", subscriptionId='" + subscriptionId + '\'' +
                ']';
    }
}
