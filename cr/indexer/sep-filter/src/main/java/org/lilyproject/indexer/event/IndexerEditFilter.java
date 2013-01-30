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
package org.lilyproject.indexer.event;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.lilyproject.util.repo.RecordEvent.IndexRecordFilterData;

import org.lilyproject.util.repo.RecordEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.lilyproject.repository.impl.id.IdGeneratorImpl;

import org.lilyproject.repository.api.IdGenerator;

import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;

import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import org.lilyproject.sep.WALEditFilter;

/**
 * Filter for SEP events that removes all KeyValues from WALEdits that are not applicable to the
 * configured index subscription.
 */
class IndexerEditFilter implements WALEditFilter {
    
    /**
     * If this attribute value is set to "false" in the RecordEvent, the SEP event will not be passed
     * through to the indexer.
     */
    public static final String NO_INDEX_FLAG = "lily.mq";

    private final IdGenerator idGenerator = new IdGeneratorImpl();
    private final Log log = LogFactory.getLog(getClass());
    private final String subscriptionName;

    /**
     * Instantiate with the name of the IndexUpdater SEP subscription for which KeyValues are to be
     * allowed. All {@code KeyValue}s in incoming {@code WALEdit}s that are not for the given index
     * will be removed.
     *
     * @param subscriptionName Name of the SEP subscription for which {@code KeyValue}s are not to be removed
     */
    public IndexerEditFilter(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    @Override
    public void apply(WALEdit walEdit) {
        List<KeyValue> keyValues = walEdit.getKeyValues();
        for (int i = keyValues.size() - 1; i >= 0; i--) {
            if (!isValidKeyValue(keyValues.get(i))) {
                keyValues.remove(i);
            }
        }
    }

    private boolean isValidKeyValue(KeyValue keyValue) {
        if (Arrays.equals(RecordCf.DATA.bytes, keyValue.getFamily())
                && Arrays.equals(RecordColumn.PAYLOAD.bytes, keyValue.getQualifier())) {
            RecordEvent recordEvent = null;
            try {
                recordEvent = new RecordEvent(keyValue.getValue(), idGenerator);
            } catch (IOException e) {
                log.error("Error parsing RecordEvent", e);
                return false;
            }
            if (recordEvent.hasAttributes() && "false".equals(recordEvent.getAttributes().get(NO_INDEX_FLAG))) {
                return false;
            }
            IndexRecordFilterData indexRecordFilterData = recordEvent.getIndexRecordFilterData();
            if (indexRecordFilterData != null) {
                return indexRecordFilterData.appliesToSubscription(subscriptionName);
            } else {
                log.warn("No IndexRecordFilterData on " + recordEvent.toJson());
            }
        }
        return false;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

}
