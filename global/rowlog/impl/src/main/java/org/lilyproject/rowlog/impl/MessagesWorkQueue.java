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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.ByteArrayKey;

public class MessagesWorkQueue {
    private final int maxMessages;
    
    private final List<RowLogMessage> messageList;
    
    private final Set<RowLogMessage> messagesWorkingOn = new HashSet<RowLogMessage>();

    private final Set<ByteArrayKey> rowsWorkingOn = new HashSet<ByteArrayKey>();
    
    /**
     * This lock must be obtained by anyone modifying the above lists, or of course when waiting/signalling
     * the conditions associated with this lock.
     */
    private final Lock lock = new ReentrantLock();

    private final Condition notFull  = lock.newCondition();

    private final Condition notEmpty = lock.newCondition();

    /**
     * If the queue contains less than this amount of messages, we'll notify that we want some fresh messages.
     */
    private final int refillThreshold = 5;

    private final Object refillTrigger = new Object();

    public MessagesWorkQueue(int size) {
        this.maxMessages = size;
        this.messageList = new LinkedList<RowLogMessage>();
    }

    public void offer(RowLogMessage message) throws InterruptedException {
        lock.lock();
        try {
            while (messageList.size() >= maxMessages) {
                notFull.await();
            }
            messageList.add(message);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Calling take() should always be matched by corresponding done() call.
     */
    public RowLogMessage take() throws InterruptedException {
        lock.lock();
        try {
            while (true) {
                while (messageList.isEmpty()) {
                    notEmpty.await();
                }

                Iterator<RowLogMessage> messages = messageList.iterator();
                while (messages.hasNext()) {
                    RowLogMessage message = messages.next();
                    ByteArrayKey row = new ByteArrayKey(message.getRowKey());
                    if (messagesWorkingOn.contains(message)) {
                        messages.remove();
                        afterMessageRemoval();
                    } else if (!rowsWorkingOn.contains(row)) {
                        messages.remove();
                        afterMessageRemoval();
                        messagesWorkingOn.add(message);
                        rowsWorkingOn.add(row);
                        return message;
                    }
                }

                // The messages list is not empty, but only contains messages for rows on which we are already working
                notEmpty.await();
            }
        } finally {
            lock.unlock();
        }
    }

    private void afterMessageRemoval() {
        notFull.signal();
        if (messageList.size() <= refillThreshold) {
            synchronized (refillTrigger) {
                refillTrigger.notifyAll();
            }
        }
    }
    
    public void done(RowLogMessage message) {
        lock.lock();
        try {
            messagesWorkingOn.remove(message);
            if (rowsWorkingOn.remove(new ByteArrayKey(message.getRowKey()))) {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
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
