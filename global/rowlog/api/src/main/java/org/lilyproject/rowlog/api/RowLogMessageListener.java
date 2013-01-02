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
 * A RowLogMessageConsumer is responsible for processing a message coming from the {@link RowLog}.
 * <p/>
 * <p> RowLogMessageConsumers should be registered on the {@link RowLog}.
 * A consumer can be asked to process a message, either by the {@link RowLog} itself when it is asked to process a message,
 * or by a {@link RowLogProcessor} that picks up a next message to be processed by a specific consumer.
 * A {@link RowLogProcessor} will only ask a consumer to process a message if the RowLogMessageConsumer was registered
 * with the {@link RowLog} before the message was put on the {@link RowLog}.
 */
public interface RowLogMessageListener {
    /**
     * Request a consumer to process a {@link RowLogMessage}.
     * <p/>
     * <p>The listener is itself responsible for retrying a message if it failed to be processed.
     * It can decide to drop the message, but then the result of this call should be true. When
     * false is returned, the message will later be picked up again by the RowLogProcessor and offered
     * again to the listener for processing.
     *
     * @param message the {@link RowLogMessage} to process
     * @return true if the listener processed the message and it should not be offered again for processing,
     *         false if the message should be re-offered again later
     * @throws InterruptedException
     */
    boolean processMessage(RowLogMessage message) throws InterruptedException;
}