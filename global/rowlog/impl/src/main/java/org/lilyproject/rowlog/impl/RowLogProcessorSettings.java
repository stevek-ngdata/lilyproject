package org.lilyproject.rowlog.impl;

import org.lilyproject.rowlog.api.RowLogProcessor;

public class RowLogProcessorSettings {
    private int scanThreadCount = -1;

    private int msgTimestampMargin = RowLogProcessor.DEFAULT_MSG_TIMESTAMP_MARGIN;

    public int getScanThreadCount() {
        return scanThreadCount;
    }

    /**
     * The scan thread count configured by the user, values < 1 mean it will be determined
     *  automatically.
     */
    public void setScanThreadCount(int scanThreadCount) {
        this.scanThreadCount = scanThreadCount;
    }

    public int getMsgTimestampMargin() {
        return msgTimestampMargin;
    }

    /**
     * Maximum expected clock skew between servers. In fact this is not just pure clock skew, but also
     * encompasses the delay between the moment of timestamp determination and actual insertion onto HBase.
     * The higher the skew, the more put-delete pairs of past messages that HBase will have to scan over.
     * The shorter the skew, the more chance messages will get stuck unprocessed, either due to clock skews
     * or due to slow processing of the put on the global queue, such as in case of HBase region recovery.
     * At the time of this writing, HBase checked this skew, allowing up to 30s:
     * https://issues.apache.org/jira/browse/HBASE-3168
     */
    public void setMsgTimestampMargin(int msgTimestampMargin) {
        this.msgTimestampMargin = msgTimestampMargin;
    }
}
