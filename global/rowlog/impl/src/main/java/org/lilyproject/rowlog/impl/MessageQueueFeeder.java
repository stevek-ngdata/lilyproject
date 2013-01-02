/*
 * Copyright 2010 Outerthought bvba
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;

public class MessageQueueFeeder implements RowLogMessageListener {
    private Log log = LogFactory.getLog(getClass());
    private RowLog messageQueue = null;

    public MessageQueueFeeder(RowLog messageQueueRowLog) {
        this.messageQueue = messageQueueRowLog;
    }

    @Override
    public boolean processMessage(RowLogMessage message) throws InterruptedException {
        Exception lastException = null;
        // When an exception occurs, we retry to put the message.
        // But only during 5 seconds since there can be a client waiting on its call to return.
        // If it fails, the message will be retried later either through the RowLogProcessor of the WAL,
        // or when an new update happens on the same record and the remaining messages are being processed first.
        for (int i = 0; i < 50; i++) {
            try {
                messageQueue.putMessage(message.getRowKey(), message.getData(), message.getPayload(), null);
                return true;
            } catch (RowLogException e) {
                lastException = e;
                Thread.sleep(100);
            }
        }
        log.info("Failed to put message '" + message + "' on the message queue. Retried during 5 seconds.", lastException);
        return false;
    }
}
