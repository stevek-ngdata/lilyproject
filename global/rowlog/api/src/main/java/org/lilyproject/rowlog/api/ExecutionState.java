/*
 * Copyright 2011 Outerthought bvba
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
 * The execution state of a rowlog message.
 * <p>
 * It keeps track for each subscription if the message has been handled or not.
 */
public interface ExecutionState {

    /**
     * Returns the timestamp of when the execution state was created
     */
    public long getTimestamp();

    /**
     * Returns the subscription Ids for which the execution state has been
     * initialized
     */
    public String[] getSubscriptionIds();

    /**
     * Converts the execution state to a byte representation to store it in
     * HBase
     */
    public byte[] toBytes();

    /**
     * Returns true when the message has been handled for each subscription
     */
    public boolean allDone();

    /**
     * Updates the state of a subscription
     */
    public void setState(String subscriptionId, boolean state);

    /**
     * Returns the state of the message for a subscription
     */
    public boolean getState(String subscriptionId);
}