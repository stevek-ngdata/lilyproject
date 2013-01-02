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

import java.io.IOException;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.rowlog.api.ExecutionState;

public class SubscriptionExecutionState implements ExecutionState {

    private final long timestamp;
    private final String[] subscriptionIds;
    private final boolean[] doneFlags;

    private static final byte FORMAT_VERSION = 1;

    public SubscriptionExecutionState(long timestamp, String[] subscriptionIds) {
        this.timestamp = timestamp;
        this.subscriptionIds = subscriptionIds;
        this.doneFlags = new boolean[subscriptionIds.length];
    }

    public SubscriptionExecutionState(long timestamp, String[] subscriptionIds, boolean[] doneFlags) {
        this.timestamp = timestamp;
        this.subscriptionIds = subscriptionIds;
        this.doneFlags = doneFlags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String[] getSubscriptionIds() {
        return subscriptionIds;
    }

    public void setState(String subscriptionId, boolean state) {
        for (int i = 0; i < subscriptionIds.length; i++) {
            if (subscriptionIds[i].equals(subscriptionId)) {
                doneFlags[i] = state;
                return;
            }
        }
        // We should never get here, since getState() returns true for unknown subscriptions
        throw new RuntimeException("setState called for undefined subscription: " + subscriptionId);
    }

    public boolean getState(String subscriptionId) {
        for (int i = 0; i < subscriptionIds.length; i++) {
            if (subscriptionIds[i].equals(subscriptionId)) {
                return doneFlags[i];
            }
        }
        return true;
    }

    public byte[] toBytes() {
        DataOutput dataOutput = new DataOutputImpl();
        // First write a version number to support future evolution of the serialization format
        dataOutput.writeByte(FORMAT_VERSION);
        dataOutput.writeLong(timestamp);
        dataOutput.writeVInt(subscriptionIds.length);

        for (int i = 0; i < subscriptionIds.length; i++) {
            dataOutput.writeUTF(subscriptionIds[i]);
            dataOutput.writeBoolean(doneFlags[i]);
        }

        return dataOutput.toByteArray();
    }

    public static SubscriptionExecutionState fromBytes(byte[] bytes) throws IOException {
        DataInput input = new DataInputImpl(bytes);
        byte version = input.readByte();

        if (version != FORMAT_VERSION) {
            throw new RuntimeException("Unsupported subscription execution state serialized format version: " +
                    (short)version);
        }

        long timestamp = input.readLong();
        int size = input.readVInt();

        String[] subscriptionIds = new String[size];
        boolean[] doneFlags = new boolean[size];

        for (int i = 0; i < size; i++) {
            subscriptionIds[i] = input.readUTF();
            doneFlags[i] = input.readBoolean();
        }

        return new SubscriptionExecutionState(timestamp, subscriptionIds, doneFlags);
    }

    public boolean allDone() {
        for (int i = 0; i < doneFlags.length; i++) {
            if (!doneFlags[i]) {
                return false;
            }
        }
        return true;
    }
}
