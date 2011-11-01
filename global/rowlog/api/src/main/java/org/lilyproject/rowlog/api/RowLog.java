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
package org.lilyproject.rowlog.api;

import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.rowlock.RowLock;

/**
 * The RowLog helps managing the execution of synchronous and asynchronous actions in response to
 * updates happening to the rows of an HBase table.
 * 
 * <p> It has been introduced as a basis to build a distributed persistent Write Ahead Log (WAL) and Message Queue (MQ) 
 * on top of HBase for the Lily Content Repository. More information about the design and rationale behind it can be found
 * on <a href="http://lilyproject.org/">http://lilyproject.org/</a>
 *
 * <p> The RowLog accepts and stores {@link RowLogMessage}s. The context of these messages is always related to a specific 
 * row in a HBase table, hence the name 'RowLog'. {@link RowLogMessageListener}s are responsible for processing the messages and
 * should be registered with the RowLog. 
 * 
 * <p> The messages are stored on, and distributed randomly over several {@link RowLogShard}s. 
 * Each shard uses one HBase table to store its messages on. And each shard is considered to be equal. It does not matter
 * if a message is stored on one or another shard.
 * (Note: the use of more than one shard is still to be implemented.) 
 * 
 * <P> For each shard, a {@link RowLogProcessor} can be started. This processor will, for each subscription, pick the next
 * message to be processed and feed it to the {@link RowLogMessageListener} for processing. Since messages are distributed
 * over several shards and each shard has its own processor, it can happen that the order in which the messages have been
 * put on the RowLog is not respected when they are sent to the listeners for processing. See below, on how to deal with
 * this.
 *  
 * <p> On top of this, a 'payload' and 'execution state' is stored for each message in the same row to which the message relates,
 * next to the row's 'main data'.
 * The payload contains all data that is needed by the listeners of a subscription to be able to process a message.
 * The execution state indicates for which subscriptions the message has been processed and which subscriptions still need to process it.
 * 
 * <p> All messages related to a certain row are given a sequence number in the order in which they are put on the RowLog.
 * This sequence number is used when storing the payload and execution state on the HBase row. 
 * This enables a {@link RowLogMessageListener} to check if the message it is requested to process is the oldest
 * message to be processed for the row or if there are other messages to be processed for the row. It can then choose
 * to, for instance, process an older message first or even bundle the processing of multiple messages together.
 */
public interface RowLog {
    
    /**
     * The id of a RowLog uniquely identifies the rowlog amongst all rowlog instances.
     */
    String getId();

    /**
     * Retrieves the payload of a {@link RowLogMessage} from the RowLog.
     * The preferred way to get the payload for a message is to request this through the message itself 
     * with the call {@link RowLogMessage#getPayload()} .
     * @param message a {@link RowLogMessage}
     * @return the payload of the message
     * @throws RowLogException
     */
    byte[] getPayload(RowLogMessage message) throws RowLogException;

    /**
     * Puts a new message on the RowLog. This will add a new message on a {@link RowLogShard} 
     * and put the payload and an execution state on the HBase row this message is about.
     *
     * <p>When used as MQ, this method may be called without taking a lock on the row. However, in such case
     * there is no guarantee that the messages will be delivered in sequence number order. You can however combine
     * both: use row-locking for the messages which should be mutually ordered, and skip the lock for
     * messages for which it doesn't matter. Other calls like {@link #getMessages} will also be influenced by this.
     *
     * @param rowKey the HBase row the message is related to
     * @param data some informative data to be put on the message
     * @param payload the information needed by a {@link RowLogMessageListener} to be able to process the message
     * @param put a HBase {@link Put} object that, when given, will be used by the RowLog to put the payload and 
     * execution state information on the HBase row. This enables the combination of the actual row data and the payload
     * and execution state to be put on the HBase row with a single, atomic, put call. When using this functionality, 
     * a {@link HTable#put(Put)} call should follow this call. 
     * @return a new {@link RowLogMessage} with a unique id and a sequence number indicating its position in the list of
     * messages of the row.
     * @throws RowLogException
     * @throws InterruptedException 
     */
    RowLogMessage putMessage(byte[] rowKey, byte[] data, byte[] payload, Put put) throws RowLogException, InterruptedException;
    
    /**
     * Request each registered {@link RowLogMessageListener} to process a {@link RowLogMessage} explicitly. 
     * This method can be called independently from a {@link RowLogProcessor} and can be used for instance when a message
     * needs to be processed immediately after it has been put on the RowLog instead of waiting for a {@link RowLogProcessor}
     * to pick it up and process it.
     * <p>When the message has been processed successfully for all subscriptions it will be removed from the rowlog and true
     * will be returned. Note: it is the listener's responsibility to handle any failures while processing a message. If a
     * message is not processed by a listener is will remain on the rowlog an will be offered again to the listeners for 
     * processing the next time the processor encounters it. 
     * @param message a {@link RowLogMessage} to be processed
     * @param lock if the row related to this message was locked with a Rowlock this lock should be given
     * @return true if all subscriptions have processed the {@link RowLogMessage} successfully
     */
    boolean processMessage(RowLogMessage message, RowLock lock) throws RowLogException, InterruptedException;
    
    /**
     * Indicates that a {@link RowLogMessage} is done for a certain {@link RowLogSubscription}
     * and should not be processed anymore. This will remove the message for that subscription from the {@link RowLogShard} 
     * and update the execution state of this message on the row. When the execution state for all subscriptions is put to
     * done, the messsage will have been removed from the shard for each subscription, and the payload and execution state 
     * will be removed from the row.
     * @param message the {@link RowLogMessage} to be put to done for a certain subscription
     * @param subscriptionId the id of the subscription for which to put the message to done
     * @return true if the message has been successfully put to done
     */
    boolean messageDone(RowLogMessage message, String subscriptionId) throws RowLogException, InterruptedException;
    
    /**
     * Checks if the message is done for a certain subscription.
     * <p>This will investigate the execution state of the message to check if the message has been processed.
     * @param message the message to check
     * @param subscriptionId id of the subscription for which to check the message
     * @return true of the message is done
     * @throws RowLogException
     */
    boolean isMessageDone(RowLogMessage message, String subscriptionId) throws RowLogException;
    
    /**
     * Return all messages that still exist for the row, or if one or more subscriptions is given, 
     * only the messages that are still open for one or more of those subscriptions.
     *
     * <p>If messages are put on this rowlog without using a rowlock, then there is no guarantee
     * about the messages which will be present in the returned list at a given instant in time.
     * For example, a call could return the messages with sequence number 5,6 and 8, and a later
     * call could then return the message with sequence number 7.
     *
     * @param rowKey the row for which to return the messages
     * @param subscriptionId one or more subscriptions for which to return the messages that are open
     * @return a list of (open)messages of the row
     * @throws RowLogException
     */
    List<RowLogMessage> getMessages(byte[] rowKey, String ... subscriptionId) throws RowLogException;

    /**
     * @return copy of the list of subscriptions on the rowlog
     */
    List<RowLogSubscription> getSubscriptions();

    /**
     * @return copy of the list of subscriptions on the rowlog
     */
    RowLogSubscription[] getSubscriptionsAsArray();

    /**
     * @return the list of registered shards on the rowlog
     */
    List<RowLogShard> getShards();

    /**
     * Checks if a message is available for processing for a certain subscription.
     * <p>A message will not be available if it is either already done, 
     * or the message needs to be processed by another subscription first in case the rowlog should
     * preserve the order of the subscription.
     * @param message the message to check
     * @param subscriptionId for which subscription to check the message
     * @return true if the message is available
     * @throws RowLogException
     */
    boolean isMessageAvailable(RowLogMessage message, String subscriptionId) throws RowLogException;
}
