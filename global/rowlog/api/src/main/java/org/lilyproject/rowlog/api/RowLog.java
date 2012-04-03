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
 * <p>It has been introduced as a basis to build a distributed persistent Write Ahead Log (WAL) and Message Queue (MQ)
 * on top of HBase for the Lily Content Repository. More information about the design and rationale behind it can be found
 * on <a href="http://lilyproject.org/">http://lilyproject.org/</a>
 *
 * <p>The RowLog accepts and stores {@link RowLogMessage}s. The context of these messages is always related to a
 * specific row in a HBase table, hence the name 'RowLog'. {@link RowLogMessageListener}s are responsible for
 * processing the messages and should be registered with the RowLog.
 *
 * <p>The messages for each row are stored on the row they are about, typically in a specific column family. This
 * is also called the "row-local queue". In order to know what rows have outstanding messages, an "index" is maintained
 * that points to those rows. This index is also known as the "global queue" or as "rowlog shard". Since the global
 * queue is time-ordered, it is sensitive to region server hotspotting (the phenomenon where you are always touching
 * the same region server because you are appending to the end of the table). For this reason, the global queue is
 * sharded (partitioned). Currently the partitioning happens over multiple splits of a pre-splitted HBase table
 * (in the implementation, see the class RowLogShardSetup).
 * This was preferred over using multiple tables, since HBase will prefer to distribute the splits of one table
 * as much as possible over different region servers.</p>
 *
 * <p>The primary reason for storing the messages about a row on the row itself is because this allows us
 * to benefit from HBase's atomic row update ability to guarantee that both a row update and the corresponding
 * message are added as one atomic operation. See also the design document.</p>
 *
 * <p>A {@link RowLogProcessor} is responsible for querying the rowlog shards for new messages, and feeding them
 * to the {@link RowLogMessageListener} for processing (and this for each subscription). Right now, there is one
 * RowLogProcessor instance responsible for all rowlog shards, thus the RowLogProcessor execution itself is not
 * sharded over multiple nodes. We felt this was an acceptable starting approach since the task of the
 * RowLogProcessor is rather lightweight, the heavy work is up to HBase and the listeners. This might be reviewed
 * in the future.
 *
 * <p>Each RowLogMessage has a payload, that is the application-specific content of the message, thus that what
 * allows an application to know what the message is about.
 *
 * <p>The payload of the messages, and their 'execution state' (bookkeeping around whether a message has been processed
 * for some subscription) is stored in the "row-local queue" mentioned above, thus in the row to which the message
 * relates. Once a message is processed by all its subscriptions, it is removed from the row-local queue.
 *
 * <p>The rowlog guarantees that it will not deliver messages of the same row concurrently to listeners of
 * the same subscription (there can be any number of listeners, spreaded on different nodes in a cluster,
 * consuming messages for a single subscription). This avoids that listeners might need to do locking if they don't
 * want to work concurrently for the same row.</p>
 *
 * <p>The rowlog will in general deliver messages in order (of timestamp across all the rows, and sequence
 * number within a row), but this is not an absolute guarantee. If a listener wants to be sure there are no
 * earlier messages about a row that it still needs to process, it can read out the row-local queue. It could
 * even decide to process in one go all the outstanding messages. This is an important advantage of having
 * a "row-local queue".</p>
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
     * @return copy of the list of subscriptions on the rowlog. The list is sorted on the subscription's
     * order number.
     */
    List<RowLogSubscription> getSubscriptions();

    /**
     * @return the list of registered shards on the rowlog
     */
    List<RowLogShard> getShards();

    RowLogShardList getShardList();

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
    
    RowLogConfig getConfig();
}
