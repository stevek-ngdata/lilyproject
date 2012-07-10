package org.lilyproject.indexer.integration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        //
        // Get a snapshot of the currently known MQ subscriptions
        //
        List<RowLogSubscription> subscriptions = new ArrayList<RowLogSubscription>(messageQueue.getSubscriptions());

        //
        // If there are indexes, remove their subscription from the list if we see the record is not
        // relevant for the index. This avoids the cost of needlessly dispatching this event to
        // indexes that don't need it anyway.
        //
        Collection<IndexInfo> indexInfos = indexesInfo.getIndexInfos();
        if (indexInfos.size() >= 0) {
            try {
                //
                // Get the RecordEvent object (optimized to avoid re-parsing)
                //
                Object context = message.getContext();
                RecordEvent recordEvent = null;
                if (context != null) {
                    RowLogContext rowLogContext = (RowLogContext)message.getContext();
                    recordEvent = rowLogContext.getRecordEvent();
                }
                if (recordEvent == null) {
                    recordEvent = new RecordEvent(message.getPayload(), repository.getIdGenerator());
                }
                
                // Check the attributes on the recordEvent to make sure that the event should be processed.
                boolean isRecordProcessable = !"false".equals(recordEvent.getAttributes().get("lily.mq"));
                if (!isRecordProcessable) {
                    // The record should not be processed so we will clear the subscriptions                 
                    subscriptions.clear();
                } else {

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
                }
            } catch (Exception e) {
                log.error("Error while performing index-aware filtering of subscriptions in the MQ feeder", e);
            }
        }

        //
        // Add event to the MQ, this is the original code from MessageQueueFeeder with an additional
        // argument to putMessage.
        //

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

    private void removeSubscription(List<RowLogSubscription> subscriptions, String idToRemove) {
        Iterator<RowLogSubscription> subscriptionIt = subscriptions.iterator();
        while (subscriptionIt.hasNext()) {
            if (subscriptionIt.next().getId().equals(idToRemove)) {
                subscriptionIt.remove();
                return;
            }
        }
    }
}

