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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.ByteArrayKey;

public class MessagesWorkQueue {
    private BlockingQueue<RowLogMessage> messageQueue = new ArrayBlockingQueue<RowLogMessage>(100);

    private Set<RowLogMessage> messagesWorkingOn = new HashSet<RowLogMessage>();
    private Set<ByteArrayKey> rowsWorkingOn = new HashSet<ByteArrayKey>();
    
    /**
     * If the queue contains less than this amount of messages, we'll notify that we want some fresh messages.
     */
    private final int refillThreshold = 5;

    private final Object refillTrigger = new Object();

    public void offer(RowLogMessage message) throws InterruptedException {
        // First check if the message is already in the queue or is being worked on
        synchronized(messagesWorkingOn) {
            if (messageQueue.contains(message) || messagesWorkingOn.contains(message)) 
                return;
        }
        // Put the message on the queue, this blocks until there is room on the messageQueue
        // This can(should) be done outside the synchronize block sine #offer is only called from one place (one thread)
        // so no one else can jump in between.
        messageQueue.put(message);
    }
    
    public RowLogMessage take() throws InterruptedException {
        // Take a message from the queue, this blocks until there is a message on the queue
        RowLogMessage message = messageQueue.take();
        // Only add new messages to the queue when the number of messages in the queue drops below the threshold
        if (messageQueue.size() <= refillThreshold) {
            synchronized (refillTrigger) {
                refillTrigger.notifyAll();
            }
        }

        // Make sure no other listener is working on the message or on the same row
        synchronized (messagesWorkingOn) {
            // In case the message was added again before we could synchronize on messagesWorkingOn,
            // remove it again to avoid that another listener would start working on it
            // Note : we can still be too late, so we need to check next it again (below)
            messageQueue.remove(message); 
            ByteArrayKey rowKey = new ByteArrayKey(message.getRowKey());
            // Make sure the message was not added again (before synchronizing) and another listener started working on it
            // And also check that no other listener is working on the same row
            if (messagesWorkingOn.contains(message) || rowsWorkingOn.contains(rowKey)) 
                return null; 
            // The message can be returned and marked that it and its row are going to be worked on
            messagesWorkingOn.add(message);
            rowsWorkingOn.add(rowKey);
            return message;
        }
    }
    
    public void done(RowLogMessage message) {
        // Done with the message, safely remove it and its row
        synchronized (messagesWorkingOn) {
            messagesWorkingOn.remove(message);
            rowsWorkingOn.remove(new ByteArrayKey(message.getRowKey()));
        }
    }
    
    public int size() {
    	return messageQueue.size();
    }

    public void waitOnRefillThreshold() throws InterruptedException {
        synchronized (refillTrigger) {
            while (messageQueue.size() > refillThreshold) {
                refillTrigger.wait();
            }
        }
    }
}
