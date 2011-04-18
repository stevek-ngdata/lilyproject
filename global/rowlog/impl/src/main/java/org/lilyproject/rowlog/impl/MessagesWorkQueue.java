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

import java.util.*;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.ByteArrayKey;

public class MessagesWorkQueue {
private static final int MAX_MESSAGES = 100;

    private final List<RowLogMessage> messageList = new ArrayList<RowLogMessage>(MAX_MESSAGES);
    
    private final Set<RowLogMessage> messagesWorkingOn = new HashSet<RowLogMessage>();
    private final Set<ByteArrayKey> rowsWorkingOn = new HashSet<ByteArrayKey>();
    
    /**
     * If the queue contains less than this amount of messages, we'll notify that we want some fresh messages.
     */
    private final int refillThreshold = 5;

    private final Object refillTrigger = new Object();

    public void offer(RowLogMessage message) throws InterruptedException {
        synchronized (messageList) {
            while (messageList.size() >= MAX_MESSAGES) {
                messageList.wait();
            }
            messageList.add(message);
            messageList.notifyAll();
        }
    }

    /**
     * Calling take() should always be matched by corresponding done() call.
     */
    public RowLogMessage take() throws InterruptedException {
        synchronized (messageList) {
            while (true) {
                while (messageList.isEmpty()) {
                    messageList.wait();
                }

                Iterator<RowLogMessage> messages = messageList.iterator();
                while (messages.hasNext()) {
                    RowLogMessage message = messages.next();
                    ByteArrayKey row = new ByteArrayKey(message.getRowKey());
                    if (messagesWorkingOn.contains(message)) {
                        messages.remove();
                        messageList.notifyAll();
                    } else if (!rowsWorkingOn.contains(row)) {
                        messages.remove();
                        messagesWorkingOn.add(message);
                        rowsWorkingOn.add(row);
                        messageList.notifyAll();
                        if (messageList.size() <= refillThreshold) {
                            synchronized (refillTrigger) {
                                refillTrigger.notifyAll();
                            }
                        }
                        return message;
                    }
                }

                // The messages list is not empty, but only contains messages for rows on which we are already working
                messageList.wait();
            }
        }
    }
    
    public void done(RowLogMessage message) {
        synchronized (messageList) {
            messagesWorkingOn.remove(message);
            if (rowsWorkingOn.remove(new ByteArrayKey(message.getRowKey()))) {
                messageList.notifyAll();
            }
        }
    }
    
    public int size() {
    	return messageList.size();
    }

    public void waitOnRefillThreshold() throws InterruptedException {
        synchronized (refillTrigger) {
            while (messageList.size() > refillThreshold) {
                refillTrigger.wait();
            }
        }
    }
}
