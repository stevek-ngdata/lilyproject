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

/**
 * The RowLogMessage is the message object that should be put on
 * the {@link RowLog} and processed by a {@link RowLogMessageListener}
 * <p/>
 * <p> A RowLogMessage is created by the {@link RowLog} when calling {@link RowLog#putMessage(byte[], byte[], byte[], org.apache.hadoop.hbase.client.Put)}
 * <p/>
 * <p> The message should contain the information needed by the consumers to be able to do their work.
 * In other words, the producer of the message and the consumers should agree on the content of the message.
 * <p/>
 * <p> A message is always related to a specific HBase-row and is used to describe an event that happened on the data of that row.
 * <p/>
 * <p> A RowLogMessage is uniquely identified by the combination of its rowKey, seqNr and timestamp.
 */
public interface RowLogMessage {
    /**
     * Identifies the row to which the message is related.
     *
     * @return the HBase row key
     */
    byte[] getRowKey();

    /**
     * A sequence number used to identify the position of the message in order of the messages that were created (events) for the related row.
     *
     * @return a sequence number , unique within the context of a row
     */
    long getSeqNr();

    /**
     * The timestamp of when the message was created.
     *
     * @return the timestamp
     */
    long getTimestamp();

    /**
     * The data field can be used to put extra informative information on the message.
     * This data will be stored in the message table and should be kept small.
     *
     * @return the data
     */
    byte[] getData();

    /**
     * The payload contains all information about a message for a {@link RowLogMessageListener} to be able to process a message.
     *
     * @return the payload
     * @throws RowLogException
     */
    byte[] getPayload() throws RowLogException;

    /**
     * Allows to add a context object to the message object when it is passed
     * along to get processed.
     */
    void setContext(Object context);

    /**
     * Returns the context object or null if none is present.
     */
    Object getContext();

    /**
     * Allows to set the execution state right after creation of the message.
     * <p/>
     * This execution state can be used (and should only be used) in those flows
     * where the message will be processed right after its creation, and where
     * no other processing of the message could already have happened.
     * <p/>
     * The execution state is no integral part of the message and will not be
     * encoded nor stored when the message is stored. It only serves as a means
     * to transport the execution that exists right after creation of the
     * message.
     */
    void setExecutionState(ExecutionState executionState);

    /**
     * Returns the execution state that is present on the message.
     */
    ExecutionState getExecutionState();
}
