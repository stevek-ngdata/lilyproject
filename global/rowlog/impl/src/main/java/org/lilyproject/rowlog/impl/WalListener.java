package org.lilyproject.rowlog.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.*;

/**
 * The WAL listener is a RowLogMessageListener which processes a message by calling the processMessage call on the Rowlog directly.
 * See {@link WalRowLog}
 * 
 * <p>This WalProcessor should be used together with the {@link WalRowLog} and {@link WalListener}
 */
public class WalListener implements RowLogMessageListener {
    private RowLog rowLog;
    private Log log = LogFactory.getLog(getClass());
    
    public WalListener(RowLog rowLog) {
        this.rowLog = rowLog;
    }
    
    @Override
    public boolean processMessage(RowLogMessage message) throws InterruptedException {
        try {
            return rowLog.processMessage(message, null);
        } catch (RowLogException e) {
            log.info("Failed to process message '" + message + "'", e);
            return false;
        }
    }
}
