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
package org.lilyproject.rowlog.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlock.RowLock;
import org.lilyproject.rowlock.RowLocker;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;

/**
 * The WAL listener is a RowLogMessageListener which processes a message by calling the processMessage call on the Rowlog directly.
 * See {@link WalRowLog}
 * <p/>
 * <p>This WalProcessor should be used together with the {@link WalRowLog}, {@link WalProcessor} and {@link WalSubscriptionHandler}
 */
public class WalListener implements RowLogMessageListener {
    public static final String ID = "WAL";

    private RowLog rowLog;
    private Log log = LogFactory.getLog(getClass());
    private final RowLocker rowLocker;

    public WalListener(RowLog rowLog, RowLocker rowLocker) {
        this.rowLog = rowLog;
        this.rowLocker = rowLocker;
    }

    @Override
    public boolean processMessage(RowLogMessage message) throws InterruptedException {
        RowLock rowLock = null;
        try {
            rowLock = rowLocker.lockRow(message.getRowKey());
            if (rowLock == null) {
                return false;
            }
            return rowLog.processMessage(message, rowLock);
        } catch (IOException e) {
            log.info("Failed to process message '" + message + "'", e);
            return false;
        } catch (RowLogException e) {
            log.info("Failed to process message '" + message + "'", e);
            return false;
        } finally {
            if (rowLock != null) {
                try {
                    rowLocker.unlockRow(rowLock);
                } catch (IOException e) {
                    log.info("Failed to unlock row after processing message '" + message + "'", e);
                }
            }
        }
    }
}
