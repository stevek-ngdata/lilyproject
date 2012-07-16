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
package org.lilyproject.indexer.integration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.indexer.model.indexerconf.IndexRecordFilter;
import org.lilyproject.indexer.model.util.IndexInfo;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.rowlog.api.RowLogMessageListenerMapping;
import org.lilyproject.rowlog.api.RowLogSubscription;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RowLogContext;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class IndexAwareMQFeeder implements RowLogMessageListener {
    private Log log = LogFactory.getLog(getClass());
    private final RowLog messageQueue;
    private final Repository repository;
    private final IndexesInfo indexesInfo;

    public IndexAwareMQFeeder(RowLog messageQueueRowLog, Repository repository, IndexesInfo indexesInfo) {
        this.messageQueue = messageQueueRowLog;
        this.repository = repository;
        this.indexesInfo = indexesInfo;
    }

    @PostConstruct
    public void register() {
        RowLogMessageListenerMapping.INSTANCE.put("MQFeeder", this);
    }

    @Override
    public boolean processMessage(RowLogMessage message) throws InterruptedException {
        // Load & parse RecordEvent
        RecordEvent recordEvent = getRecordEvent(message);
        if (recordEvent == null) {
            return true;
        }

        // Don't feed MQ if attribute lily.mq=false is set
        if ("false".equals(recordEvent.getAttributes().get("lily.mq"))) {
            return true;
        }

        // Get a snapshot of the currently known MQ subscriptions
        List<RowLogSubscription> subscriptions = new ArrayList<RowLogSubscription>(messageQueue.getSubscriptions());

        // If there are indexes, remove their subscription from the list if we see the record is not
        // relevant for the index. This avoids the cost of needlessly dispatching this event to
        // indexes that don't need it anyway.
        Collection<IndexInfo> indexInfos = indexesInfo.getIndexInfos();
        if (indexInfos.size() >= 0) {
            filterSubscriptions(subscriptions, indexInfos, message, recordEvent);
        }

        // Add event to the MQ
        return addMessageToMQ(message, subscriptions);
    }

    private void removeSubscription(List<RowLogSubscription> subscriptions, String idToRemove) {
        Iterator<RowLogSubscription> subscriptionIt = subscriptions.iterator();
        while (subscriptionIt.hasNext()) {
            if (subscriptionIt.next().getId().equals(idToRemove)) {
                subscriptionIt.remove();
                return;
            }
        }
    }

    private RecordEvent getRecordEvent(RowLogMessage message) {
        // Get the RecordEvent object (optimized to avoid re-parsing)
        Object context = message.getContext();
        RecordEvent recordEvent = null;
        if (context != null) {
            RowLogContext rowLogContext = (RowLogContext)message.getContext();
            recordEvent = rowLogContext.getRecordEvent();
        }
        if (recordEvent == null) {
            try {
                recordEvent = new RecordEvent(message.getPayload(), repository.getIdGenerator());
            } catch (Exception e) {
                log.error("Error loading or parsing record event for record " +
                        Bytes.toStringBinary(message.getRowKey()), e);
            }
        }
        return recordEvent;
    }

    /**
     * Performs the index-aware filtering of the subscriptions, modifies the provided subscriptions
     * list.
     */
    private void filterSubscriptions(List<RowLogSubscription> subscriptions, Collection<IndexInfo> indexInfos,
            RowLogMessage message, RecordEvent recordEvent) {

        try {
            //
            // Create the 'old' and 'new' Record instances.
            //
            // The RecordEvent contains the fields & record type info needed by the filters of the different
            // indexes, this is taken care of by IndexSelectionRecordUpdateHook.
            //
            // Of course, it can happen that indexerconfs have been changed, or new indexes have been added,
            // since the event was created, and then this information will be missing. We could check for that
            // and in case of doubt send the event to the index anyway. This approach however also has the
            // disadvantage that, in case there are a lot of outstanding events in the queue, that they
            // might be sent to indexes that only expect a low update rate. Besides, it also complicates the
            // code. So we go for the simple approach: when the indexerconfs change, there is a transition
            // period to be expected, and one might need to rebuild indexes.
            //
            RecordId recordId = repository.getIdGenerator().fromBytes(message.getRowKey());
            Record newRecord = repository.newRecord(recordId);
            Record oldRecord = repository.newRecord(recordId);

            RecordEvent.IndexSelection idxSel = recordEvent.getIndexSelection();
            if (idxSel != null) {
                TypeManager typeManager = repository.getTypeManager();

                if (idxSel.getFieldChanges() != null) {
                    for (RecordEvent.FieldChange fieldChange : idxSel.getFieldChanges()) {
                        FieldType fieldType = typeManager.getFieldTypeById(fieldChange.getId());
                        QName name = fieldType.getName();

                        if (fieldChange.getNewValue() != null) {
                            Object value = fieldType.getValueType().read(fieldChange.getNewValue());
                            newRecord.setField(name, value);
                        }

                        if (fieldChange.getOldValue() != null) {
                            Object value = fieldType.getValueType().read(fieldChange.getOldValue());
                            oldRecord.setField(name, value);
                        }
                    }
                }

                if (idxSel.getNewRecordType() != null) {
                    newRecord.setRecordType(typeManager.getRecordTypeById(idxSel.getNewRecordType(), null).getName());
                }

                if (idxSel.getOldRecordType() != null) {
                    oldRecord.setRecordType(typeManager.getRecordTypeById(idxSel.getOldRecordType(), null).getName());
                }
            }

            //
            // And now, the actual subscription filtering
            //
            for (IndexInfo indexInfo : indexInfos) {
                // If the filter of the indexerconf matches either the old or new record state,
                // then the index needs to process this event
                boolean relevantIndex = false;
                IndexRecordFilter filter = indexInfo.getIndexerConf().getRecordFilter();
                if (filter.getIndexCase(oldRecord) != null || filter.getIndexCase(newRecord) != null) {
                    relevantIndex = true;
                }

                // If not relevant, remove it from the list of subscriptions
                if (!relevantIndex) {
                    String subscriptionId = indexInfo.getIndexDefinition().getQueueSubscriptionId();
                    removeSubscription(subscriptions, subscriptionId);
                }
            }
        } catch (Exception e) {
            log.error("Error while performing index-aware filtering of subscriptions in the MQ feeder", e);
        }
    }

    /**
     * Add message to the MQ, this is the code from the original MessageQueueFeeder.
     */
    private boolean addMessageToMQ(RowLogMessage message, List<RowLogSubscription> subscriptions)
            throws InterruptedException {
        Exception lastException = null;
        // When an exception occurs, we retry to put the message.
        // But only during 5 seconds since there can be a client waiting on its call to return.
        // If it fails, the message will be retried later either through the RowLogProcessor of the WAL,
        // or when an new update happens on the same record and the remaining messages are being processed first.
        for (int i = 0; i < 50; i++) {
            try {
                messageQueue.putMessage(message.getRowKey(), message.getData(), message.getPayload(), null,
                        subscriptions);
                return true;
            } catch (RowLogException e) {
                lastException = e;
                Thread.sleep(100);
            }
        }
        log.info("Failed to put message '" + message + "' on the message queue. Retried during 5 seconds.",
                lastException);
        return false;
    }
}

