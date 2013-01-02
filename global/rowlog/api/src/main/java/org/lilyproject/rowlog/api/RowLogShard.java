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

/**
 * A RowLogShard is a shard of the "global rowlog queue", that is the index that points to the rows
 * that might have outstanding messages to be processed. The reason why this is sharded is explained
 * over at {@link RowLog}.
 * <p/>
 * <p>A RowLogShard is added to the {@link RowLogShardList} obtained from {@link RowLog#getShardList()}</p>
 */
public interface RowLogShard {

    /**
     * The id of a RowLogShard uniquely identifies a shard in the context of a {@link RowLog}
     */
    String getId();

    /**
     * Puts a RowLogMessage onto the table.
     *
     * @param message the {@link RowLogMessage} to be put on the table
     * @throws RowLogException when an unexpected exception occurs
     */
    void putMessage(RowLogMessage message, List<String> subscriptionIds) throws RowLogException;

    /**
     * Puts a RowLogMessage onto the table.
     */
    void putMessage(RowLogMessage message) throws RowLogException;

    /**
     * Removes the RowLogMessage from the table for the indicated subscription.
     *
     * @param message      the {@link RowLogMessage} to be removed from the table
     * @param subscription the id of the subscription for which the message needs to be removed
     * @throws RowLogException when an unexpected exception occurs
     */
    void removeMessage(RowLogMessage message, String subscription) throws RowLogException;

    void flushMessageDeleteBuffer() throws RowLogException;

    /**
     * Retrieves the next messages to be processed by the indicated subscription.
     *
     * @param subscription the id of the subscription for which the next messages should be retrieved
     * @param batchSize    how many messages to fetch (at most)
     * @return the next batchSize, or less, {@link RowLogMessage}s to be processed
     * @throws RowLogException when an unexpected exception occurs
     */
    List<RowLogMessage> next(String subscription, int batchSize) throws RowLogException;

    /**
     * Retrieves the next messages to be processed by the indicated subscription.
     *
     * @param subscription     the id of the subscription for which the next messages should be retrieved
     * @param minimalTimestamp the minimal timestamp of the messages to be retrieved
     * @param batchSize        how many messages to fetch (at most)
     * @return the next batchSize, or less, {@link RowLogMessage}s to be processed
     * @throws RowLogException when an unexpected exception occurs
     */
    List<RowLogMessage> next(String subscription, Long minimalTimestamp, int batchSize) throws RowLogException;
}
