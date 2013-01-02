/*
 * Copyright 2012 NGDATA nv
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

import com.google.common.base.Objects;

public class RowLogConfig {

    private boolean respectOrder;
    private boolean enableNotify;
    private long notifyDelay;
    private long minimalProcessDelay;
    private long wakeupTimeout;
    private long orphanedMessageDelay;
    private int deleteBufferSize;

    /**
     * A value object bundling the configuration paramaters for a rowlog and its processors.
     *
     * @param respsectOrder        true if the order of subscriptions needs to be respected for the rowlog
     * @param enableNotify         true if the processor need to be notified of new messages being put on the rowlog
     * @param notifyDelay          the minimal delay between two notify messages to be sent to the processor
     * @param minimalProcessDelay  the minimal age a messages needs to have before a processor will pick it up for processing
     * @param wakeupTimeout        the maximum time to wait before checking for new messages in case notify messages are
     *                             missed notifying is disabled
     * @param orphanedMessageDelay time that should have passed before deciding that an entry on the global queue is
     *                             orphaned, i.e. has no corresponding message on the row-local queue.
     * @param deleteBufferSize     number of deletes to buffer per rowlog shard before applying them to HBase.
     * @see RowLogConfigurationManager
     */
    public RowLogConfig(boolean respsectOrder, boolean enableNotify, long notifyDelay, long minimalProcessDelay,
                        long wakeupTimeout, long orphanedMessageDelay, int deleteBufferSize) {
        this.respectOrder = respsectOrder;
        this.enableNotify = enableNotify;
        this.notifyDelay = notifyDelay;
        this.minimalProcessDelay = minimalProcessDelay;
        this.wakeupTimeout = wakeupTimeout;
        this.orphanedMessageDelay = orphanedMessageDelay;
        this.deleteBufferSize = deleteBufferSize;
    }

    public boolean isRespectOrder() {
        return respectOrder;
    }

    public void setRespectOrder(boolean respectOrder) {
        this.respectOrder = respectOrder;
    }

    public boolean isEnableNotify() {
        return enableNotify;
    }

    public void setEnableNotify(boolean enableNotify) {
        this.enableNotify = enableNotify;
    }

    public long getNotifyDelay() {
        return notifyDelay;
    }

    public void setNotifyDelay(long notifyDelay) {
        this.notifyDelay = notifyDelay;
    }

    public long getMinimalProcessDelay() {
        return minimalProcessDelay;
    }

    public void setMinimalProcessDelay(long minimalProcessDelay) {
        this.minimalProcessDelay = minimalProcessDelay;
    }

    public long getWakeupTimeout() {
        return wakeupTimeout;
    }

    public void setWakeupTimeout(long wakeupTimeout) {
        this.wakeupTimeout = wakeupTimeout;
    }

    public long getOrphanedMessageDelay() {
        return orphanedMessageDelay;
    }

    public void setOrphanedMessageDelay(long orphanedMessageDelay) {
        this.orphanedMessageDelay = orphanedMessageDelay;
    }

    public int getDeleteBufferSize() {
        return deleteBufferSize;
    }

    public void setDeleteBufferSize(int deleteBufferSize) {
        this.deleteBufferSize = deleteBufferSize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(enableNotify, minimalProcessDelay, notifyDelay, wakeupTimeout, orphanedMessageDelay,
                respectOrder, deleteBufferSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RowLogConfig other = (RowLogConfig)obj;
        return Objects.equal(enableNotify, other.enableNotify)
                && Objects.equal(minimalProcessDelay, other.minimalProcessDelay)
                && Objects.equal(notifyDelay, other.notifyDelay)
                && Objects.equal(wakeupTimeout, other.wakeupTimeout)
                && Objects.equal(orphanedMessageDelay, other.orphanedMessageDelay)
                && Objects.equal(respectOrder, other.respectOrder)
                && Objects.equal(deleteBufferSize, other.deleteBufferSize);
    }

    @Override
    public String toString() {
        return "RowLogConfig [respectOrder=" + respectOrder + ", enableNotify="
                + enableNotify + ", notifyDelay=" + notifyDelay + ", minimalProcessDelay=" + minimalProcessDelay +
                ", wakeupTimeout=" + wakeupTimeout + ", orphanedMessageDelay=" + orphanedMessageDelay +
                ", deleteBufferSize=" + deleteBufferSize + "]";
    }
}
