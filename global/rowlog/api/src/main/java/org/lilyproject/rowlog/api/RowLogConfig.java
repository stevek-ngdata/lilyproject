package org.lilyproject.rowlog.api;

public class RowLogConfig {

    private boolean respectOrder;
    private boolean enableNotify;
    private long notifyDelay;
    private long minimalProcessDelay;
    private long wakeupTimeout;
    private long orphanedMessageDelay;

    /**
     * A value object bundling the configuration paramaters for a rowlog and its processors.
     * @see RowLogConfigurationManager
     * 
     * @param respsectOrder true if the order of subscriptions needs to be respected for the rowlog
     * @param enableNotify true if the processor need to be notified of new messages being put on the rowlog
     * @param notifyDelay the minimal delay between two notify messages to be sent to the processor
     * @param minimalProcessDelay the minimal age a messages needs to have before a processor will pick it up for processing
     * @param wakeupTimeout the maximum time to wait before checking for new messages in case notify messages are
     *                      missed notifying is disabled
     * @param orphanedMessageDelay time that should have passed before deciding that an entry on the global queue is
     *                             orphaned, i.e. has no corresponding message on the row-local queue.
     */
    public RowLogConfig(boolean respsectOrder, boolean enableNotify, long notifyDelay, long minimalProcessDelay,
            long wakeupTimeout, long orphanedMessageDelay) {
        this.respectOrder = respsectOrder;
        this.enableNotify = enableNotify;
        this.notifyDelay = notifyDelay;
        this.minimalProcessDelay = minimalProcessDelay;
        this.wakeupTimeout = wakeupTimeout;
        this.orphanedMessageDelay = orphanedMessageDelay;
    }

    public boolean isRespectOrder() {
        return respectOrder;
    }
    
    public void setRespectOrder(boolean respectOrder) {
        this.respectOrder = respectOrder;
    }
    
    public boolean isEnableNotify() {
        return enableNotify;
    }

    public void setEnableNotify(boolean enableNotify) {
        this.enableNotify = enableNotify;
    }

    public long getNotifyDelay() {
        return notifyDelay;
    }

    public void setNotifyDelay(long notifyDelay) {
        this.notifyDelay = notifyDelay;
    }

    public long getMinimalProcessDelay() {
        return minimalProcessDelay;
    }

    public void setMinimalProcessDelay(long minimalProcessDelay) {
        this.minimalProcessDelay = minimalProcessDelay;
    }
    
    public long getWakeupTimeout() {
        return wakeupTimeout;
    }
    
    public void setWakeupTimeout(long wakeupTimeout) {
        this.wakeupTimeout = wakeupTimeout;
    }

    public long getOrphanedMessageDelay() {
        return orphanedMessageDelay;
    }

    public void setOrphanedMessageDelay(long orphanedMessageDelay) {
        this.orphanedMessageDelay = orphanedMessageDelay;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (enableNotify ? 1231 : 1237);
        result = prime * result + (int) (minimalProcessDelay ^ (minimalProcessDelay >>> 32));
        result = prime * result + (int) (notifyDelay ^ (notifyDelay >>> 32));
        result = prime * result + (int) (wakeupTimeout ^ (wakeupTimeout >>> 32));
        result = prime * result + (int) (orphanedMessageDelay ^ (orphanedMessageDelay >>> 32));
        result = prime * result + (respectOrder ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowLogConfig other = (RowLogConfig) obj;
        if (enableNotify != other.enableNotify)
            return false;
        if (minimalProcessDelay != other.minimalProcessDelay)
            return false;
        if (notifyDelay != other.notifyDelay)
            return false;
        if (wakeupTimeout != other.wakeupTimeout)
            return false;
        if (orphanedMessageDelay != other.orphanedMessageDelay)
            return false;
        if (respectOrder != other.respectOrder)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowLogConfig [respectOrder=" + respectOrder + ", enableNotify="
                + enableNotify + ", notifyDelay=" + notifyDelay + ", minimalProcessDelay=" + minimalProcessDelay +
                ", wakeupTimeout=" + wakeupTimeout + ", orphanedMessageDelay=" + orphanedMessageDelay + "]";
    }
}
