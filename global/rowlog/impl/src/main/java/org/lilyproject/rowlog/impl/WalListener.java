package org.lilyproject.rowlog.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.*;

/**
 * The WAL listener is a RowLogMessageListener which processes a message by calling the processMessage call on the Rowlog directly.
 * See {@link WalRowLog}
 * 
 * <p>This WalProcessor should be used together with the {@link WalRowLog}, {@link WalProcessor} and {@link WalSubscriptionHandler} 
 */
public class WalListener implements RowLogMessageListener {
    public static String ID = "WAL";
    
    private RowLog rowLog;
    private Log log = LogFactory.getLog(getClass());
    private final RowLocker rowLocker;
    
    public WalListener(RowLog rowLog, RowLocker rowLocker) {
        this.rowLog = rowLog;
        this.rowLocker = rowLocker;
    }
    
    public boolean processMessage(RowLogMessage message) throws InterruptedException {
        RowLock rowLock = null;
        try {
            rowLock = rowLocker.lockRow(message.getRowKey());
            if (rowLock == null)
                return false;
            return rowLog.processMessage(message, rowLock);
        } catch (IOException e) {
            log.info("Failed to process message '" + message + "'", e);
            return false;
        } catch (RowLogException e) {
            log.info("Failed to process message '" + message + "'", e);
            return false;
        } finally {
            try {
                rowLocker.unlockRow(rowLock);
            } catch (IOException e) {
                log.info("Failed to unlock row after processing message '" + message + "'", e);
            }
        }
    }
}
