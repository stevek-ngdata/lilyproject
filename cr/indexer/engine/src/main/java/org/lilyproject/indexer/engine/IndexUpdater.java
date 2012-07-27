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
package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.indexer.derefmap.DependantRecordIdsIterator;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.indexer.model.util.IndexRecordFilterUtil;
import org.lilyproject.linkindex.LinkIndexException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEventHelper;
import org.lilyproject.util.repo.VTaggedRecord;

import static org.lilyproject.util.repo.RecordEvent.Type.CREATE;
import static org.lilyproject.util.repo.RecordEvent.Type.DELETE;
import static org.lilyproject.util.repo.RecordEvent.Type.INDEX;

//
// About the exception handling strategy
// =====================================
//
// When some exception occurs, we can handle it in several ways:
//
//  * ignore it:
//       this is suitable for to be expected exceptions, such as a RecordNotFoundException. Such cases are part of
//       the normal flow and don't need to be logged. This does require putting catch blocks where needed.
//
//  * block:
//       this is what we do in case Solr is not reachable. It doesn't make sense to skip such error, since we'll
//       run into the same problem with the next message. Therefore, we wait and retry (indefinitely) until it
//       succeeds. A metric should be augmented so that the admin can be aware this is happening.
//
//       Note that we don't retry in case operations fail due to HBase IO problems, since the HBase client libraries
//       internally do already extensive retryal of operations.
//
//  * skip:
//       this is basically the same as the next case, except that it happens somewhere in a loop where we can still
//       meaningfully try to continue with the next iteration. In such case, the error should be logged and metrics
//       augmented, just as for the 'fail' case.
//
//  * fail:
//       this is for all other exceptions. The indexing of the record failed, which means the index of that record
//       will not have been updated, or that the update of the denormalized index data did not happen. In such case
//       we should clearly log this, and augment some metric so that the admin can know about it.
//
// Also be careful to let InterruptedException pass through (or to reset the interrupted flag).
//

/**
 * Updates the index in response to repository events.
 */
public class IndexUpdater implements RowLogMessageListener {
    private Repository repository;
    private Indexer indexer;
    private IndexUpdaterMetrics metrics;
    private ClassLoader myContextClassLoader;
    private IndexLocker indexLocker;
    private RowLog rowLog;
    private String subscriptionId;

    /**
     * Deref map used to update denormalized data. It is <code>null</code> in case the indexer configuration doesn't
     * contain dereference expressions.
     */
    private DerefMap derefMap;

    private Log log = LogFactory.getLog(getClass());
    private IdGenerator idGenerator;

    /**
     * @param rowLog this should be the message queue
     * @param subscriptionId ID of the rowlog subscription to which this listener is listening. This is needed
     *                       because the IndexUpdater generates events itself, which should only be sent to
     *                       this subscription.
     */
    public IndexUpdater(Indexer indexer, Repository repository, IndexLocker indexLocker,
            RowLog rowLog, IndexUpdaterMetrics metrics, DerefMap derefMap, String subscriptionId)
            throws RowLogException, IOException {
        this.indexer = indexer;
        this.repository = repository;
        this.idGenerator = repository.getIdGenerator();
        this.indexLocker = indexLocker;
        this.rowLog = rowLog;
        this.derefMap = derefMap;
        this.subscriptionId = subscriptionId;

        this.myContextClassLoader = Thread.currentThread().getContextClassLoader();

        this.metrics = metrics;
    }

    @Override
    public boolean processMessage(RowLogMessage msg) throws InterruptedException {
        long before = System.currentTimeMillis();

        // During the processing of this message, we switch the context class loader to the one
        // of the Kauri module to which the index updater belongs. This is necessary for Tika
        // to find its parser implementations.

        RecordEvent event = null;
        RecordId recordId = null;

        ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(myContextClassLoader);
            event = new RecordEvent(msg.getPayload(), idGenerator);
            recordId = idGenerator.fromBytes(msg.getRowKey());

            if (log.isDebugEnabled()) {
                log.debug("Received message: " + event.toJson());
            }

            if (event.getType().equals(INDEX)) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: reindex requested for these vtags: %2$s", recordId,
                            indexer.vtagSetToNameString(event.getVtagsToIndex())));
                }
                index(recordId, event.getVtagsToIndex());
            } else if (event.getType().equals(DELETE)) {
                // Record is deleted: delete its index entry. We do not check for a matching index case, since
                // we can't (record is not available anymore), and besides IndexAwareMQFeeder takes care of sending us
                // only relevant events.
                indexLocker.lock(recordId);
                try {
                    indexer.delete(recordId);
                } finally {
                    indexLocker.unlockLogFailure(recordId);
                }

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: deleted from index (if present) because of " +
                            "delete record event", recordId));
                }

                // After this we can go to update denormalized data
                if (derefMap != null)
                    updateDenormalizedData(recordId, null, null);
            } else { // CREATE or UPDATE
                VTaggedRecord vtRecord;

                // Based on the partial old/new record state stored in the RecordEvent, determine whether we
                // now match a different IndexCase than before, and if so, if the new case would have less vtags
                // than the old one, perform the necessary deletes on Solr.
                Record[] records =
                        IndexRecordFilterUtil.getOldAndNewRecordForRecordFilterEvaluation(recordId, event, repository);
                Record oldRecord = records[0];
                Record newRecord = records[1];
                IndexCase caseOld = oldRecord != null ? indexer.getConf().getIndexCase(oldRecord) : null;
                IndexCase caseNew = newRecord != null ? indexer.getConf().getIndexCase(newRecord) : null;

                if (oldRecord != null && newRecord != null) {
                    if (caseOld != null && caseNew != null) {
                        Set<SchemaId> droppedVtags = new HashSet<SchemaId>(caseOld.getVersionTags());
                        droppedVtags.removeAll(caseNew.getVersionTags());

                        if (droppedVtags.size() > 0) {
                            // Perform deletes
                            for (SchemaId vtag : droppedVtags) {
                                indexer.delete(recordId, vtag);
                            }
                        }
                    }
                }

                // This is an optimization: an IndexCase with empty vtags list means that this record is
                // included in this index only to trigger updating of denormalized data.
                boolean doIndexing = true;
                if (caseNew != null) {
                    doIndexing = caseNew.getVersionTags().size() > 0;
                } else if (caseNew == null && caseOld != null) {
                    // caseNew == null means either the record has been deleted, or means the record does
                    // not match the recordFilter anymore. In either case, we only need to trigger update
                    // of denormalized data (if the vtags list was empty on caseOld).
                    doIndexing = caseOld.getVersionTags().size() > 0;
                }

                RecordEventHelper eventHelper = new RecordEventHelper(event, null, repository.getTypeManager());

                if (doIndexing) {
                    indexLocker.lock(recordId);
                    try {
                        try {
                            // Read the vtags of the record. Note that while this algorithm is running, the record can
                            // meanwhile undergo changes. However, we continuously work with the snapshot of the vtags
                            // mappings read here. The processing of later events will bring the index up to date with
                            // any new changes.
                            vtRecord = new VTaggedRecord(recordId, eventHelper, repository);
                        } catch (RecordNotFoundException e) {
                            // The record has been deleted in the meantime.
                            // For now, we do nothing, when the delete event is received the record will be removed
                            // from the index (as well as update of denormalized data).
                            // TODO: we should process all outstanding messages for the record (up to delete) in one go
                            return true;
                        }

                        handleRecordCreateUpdate(vtRecord);
                    } finally {
                        indexLocker.unlockLogFailure(recordId);
                    }
                }

                if (derefMap != null) {
                    updateDenormalizedData(recordId, eventHelper.getUpdatedFieldsByScope(),
                            eventHelper.getModifiedVTags());
                }
            }

        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            if (recordId != null) {
                String eventType = event != null && event.getType() != null ? event.getType().toString() : "(unknown)";
                log.error("Failure in IndexUpdater. Record '" + recordId + "', event type " + eventType, e);
                metrics.errors.inc();
            } else {
                log.error("Failure in IndexUpdater. Failed before/while reading payload.", e);
                metrics.errors.inc();
            }
        } finally {
            long after = System.currentTimeMillis();
            metrics.updates.inc(after - before);
            Thread.currentThread().setContextClassLoader(currentCL);
        }
        return true;
    }

    private void handleRecordCreateUpdate(VTaggedRecord vtRecord) throws Exception {
        RecordEvent event = vtRecord.getRecordEvent();
        Map<Long, Set<SchemaId>> vtagsByVersion = vtRecord.getVTagsByVersion();

        // Determine the IndexCase:
        //  The indexing of all versions is determined by the record type of the non-versioned scope.
        //  This makes that the indexing behavior of all versions is equal, and can be changed (the
        //  record type of the versioned scope is immutable).
        IndexCase indexCase = indexer.getConf().getIndexCase(vtRecord.getRecord());

        if (indexCase == null) {
            // The record should not be indexed

            // The record might however have been previously indexed, therefore we need to perform a delete
            // (note that IndexAwareMQFeeder only sends events to us if either the current or old record
            //  state matched the inclusion rules)
            indexer.delete(vtRecord.getId());

            if (log.isDebugEnabled()) {
                log.debug(String.format("Record %1$s: no index case found, record deleted from index.",
                        vtRecord.getId()));
            }

            // The data from this record might be denormalized into other index entries
            // After this we go to update denormalized data
        } else {
            final Set<SchemaId> vtagsToIndex;

            if (event.getType().equals(CREATE)) {
                // New record: just index everything
                vtagsToIndex = indexer.retainExistingVtagsOnly(indexCase.getVersionTags(), vtRecord);
                // After this we go to the indexing

            } else if (event.getRecordTypeChanged()) {
                // When the record type changes, the rules to index (= the IndexCase) change

                // Delete everything: we do not know the previous record type, so we do not know what
                // version tags were indexed, so we simply delete everything
                indexer.delete(vtRecord.getId());

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: deleted existing entries from index (if present) " +
                            "because of record type change", vtRecord.getId()));
                }

                // Reindex all needed vtags
                vtagsToIndex = indexer.retainExistingVtagsOnly(indexCase.getVersionTags(), vtRecord);

                // After this we go to the indexing
            } else { // a normal update

                //
                // Handle changes to non-versioned fields
                //
                if (indexer.getConf().changesAffectIndex(vtRecord, Scope.NON_VERSIONED)) {
                    vtagsToIndex = indexer.retainExistingVtagsOnly(indexCase.getVersionTags(), vtRecord);
                    // After this we go to the treatment of changed vtag fields
                    if (log.isDebugEnabled()) {
                        log.debug(
                                String.format("Record %1$s: non-versioned fields changed, will reindex all vtags.",
                                        vtRecord.getId()));
                    }
                } else {
                    vtagsToIndex = new HashSet<SchemaId>();
                }

                //
                // Handle changes to versioned(-mutable) fields
                //
                // If there were non-versioned fields changed, then we already reindex all versions
                // so this can be skipped.
                //
                // In the case of newly created versions that should be indexed: this will often be
                // accompanied by corresponding changes to vtag fields, which are handled next, and in which case
                // it would work as well if this code would not be here.
                //
                if (vtagsToIndex.isEmpty() && (event.getVersionCreated() != -1 || event.getVersionUpdated() != -1)) {
                    if (indexer.getConf().changesAffectIndex(vtRecord, Scope.VERSIONED)
                            || indexer.getConf().changesAffectIndex(vtRecord, Scope.VERSIONED_MUTABLE)) {

                        long version =
                                event.getVersionCreated() != -1 ? event.getVersionCreated() : event.getVersionUpdated();
                        if (vtagsByVersion.containsKey(version)) {
                            Set<SchemaId> tmp = new HashSet<SchemaId>();
                            tmp.addAll(indexCase.getVersionTags());
                            tmp.retainAll(vtagsByVersion.get(version));
                            vtagsToIndex.addAll(tmp);

                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Record %1$s: versioned(-mutable) fields changed, will " +
                                        "index for all tags of modified version %2$s that require indexing: %3$s",
                                        vtRecord.getId(), version, indexer.vtagSetToNameString(vtagsToIndex)));
                            }
                        }
                    }
                }

                //
                // Handle changes to vtag fields themselves
                //
                Map<SchemaId, Long> vtags = vtRecord.getVTags();
                Set<SchemaId> changedVTagFields = vtRecord.getRecordEventHelper().getModifiedVTags();
                // Remove the vtags which are going to be reindexed anyway
                changedVTagFields.removeAll(vtagsToIndex);
                for (SchemaId vtag : changedVTagFields) {
                    if (vtags.containsKey(vtag) && indexCase.getVersionTags().contains(vtag)) {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: will index for created or updated vtag %2$s",
                                    vtRecord.getId(), indexer.safeLoadTagName(vtag)));
                        }
                        vtagsToIndex.add(vtag);
                    } else {
                        // The vtag does not exist anymore on the document, or does not need to be indexed: delete from index
                        indexer.delete(vtRecord.getId(), vtag);
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: deleted from index for deleted vtag %2$s",
                                    vtRecord.getId(), indexer.safeLoadTagName(vtag)));
                        }
                    }
                }
            }


            //
            // Index
            //
            indexer.index(vtRecord, vtagsToIndex);
        }
    }

    private void updateDenormalizedData(RecordId recordId, Map<Scope, Set<FieldType>> updatedFieldsByScope,
                                        Set<SchemaId> changedVTagFields)
            throws RepositoryException, InterruptedException, LinkIndexException, IOException {

        Multimap<RecordId, SchemaId> referrersAndVTags = ArrayListMultimap.create();

        Set<SchemaId> allVTags = indexer.getConf().getVtags();

        if (log.isDebugEnabled()) {
            log.debug("Updating denormalized data for " + recordId + ", vtags: " + changedVTagFields);
        }

        for (SchemaId vtag : allVTags) {
            if ((changedVTagFields != null && changedVTagFields.contains(vtag)) || updatedFieldsByScope == null) {
                // changed vtags or delete: reindex regardless of fields
                DependantRecordIdsIterator dependants = derefMap.findDependantsOf(recordId);
                if (log.isDebugEnabled()) {
                    log.debug("changed vtag: dependants of " + recordId + ": " +
                            depIds(derefMap.findDependantsOf(recordId)));
                }
                while (dependants.hasNext()) {
                    referrersAndVTags.put(dependants.next(), vtag);
                }
            } else {
                // vtag didn't change, but some fields did change:
                for (Scope scope : updatedFieldsByScope.keySet()) {
                    final Set<SchemaId> fields = toFields(updatedFieldsByScope.get(scope));
                    final DependantRecordIdsIterator dependants = derefMap.findDependantsOf(recordId, fields, vtag);
                    while (dependants.hasNext()) {
                        referrersAndVTags.put(dependants.next(), vtag);
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Record %1$s: found %2$s records (times vtags) to be updated because they " +
                    "might contain outdated denormalized data." +
                    " The records are: %3$s", recordId,
                    referrersAndVTags.keySet().size(), referrersAndVTags.keySet()));
        }

        //
        // Now add an index message to each of the found referrers, their actual indexing
        // will be triggered by the message queue.
        //
        for (RecordId referrer : referrersAndVTags.keySet()) {

            RecordEvent payload = new RecordEvent();
            payload.setType(INDEX);
            for (SchemaId vtag : referrersAndVTags.get(referrer)) {
                payload.addVTagToIndex(vtag);
            }

            // TODO how will this behave if the row was meanwhile deleted?
            try {
                rowLog.putMessage(referrer.toBytes(), null, payload.toJsonBytes(), null,
                        Collections.singletonList(subscriptionId));
            } catch (Exception e) {
                // We failed to put the message: this is pretty important since it means the record's index
                // won't get updated, therefore log as error, but after this we continue with the next one.
                log.error("Error putting index message on queue of record " + referrer, e);
                metrics.errors.inc();
            }
        }
    }

    private Set<SchemaId> toFields(Set<FieldType> fieldTypes) {
        return new HashSet<SchemaId>(Collections2.transform(fieldTypes, new Function<FieldType, SchemaId>() {
            @Override
            public SchemaId apply(FieldType input) {
                return input.getId();
            }
        }));
    }

    private List<RecordId> depIds(DependantRecordIdsIterator dependants) throws IOException {
        List<RecordId> recordIds = Lists.newArrayList();
        while (dependants.hasNext()) {
            recordIds.add(dependants.next());
        }
        return recordIds;
    }

    /**
     * Index a record for all the specified vtags.
     *
     * @throws IOException
     */
    private void index(RecordId recordId, Set<SchemaId> vtagsToIndex) throws RepositoryException, InterruptedException,
            SolrClientException, ShardSelectorException, IndexLockException, IOException {
        boolean lockObtained = false;
        try {
            indexLocker.lock(recordId);
            lockObtained = true;

            VTaggedRecord vtRecord;
            try {
                vtRecord = new VTaggedRecord(recordId, repository);
            } catch (RecordNotFoundException e) {
                // can't index what doesn't exist
                return;
            }

            IndexCase indexCase = indexer.getConf().getIndexCase(vtRecord.getRecord());

            if (indexCase == null) {
                return;
            }

            // Only keep vtags which should be indexed
            vtagsToIndex.retainAll(indexCase.getVersionTags());
            // Only keep vtags which exist on the record
            vtagsToIndex.retainAll(vtRecord.getVTags().keySet());

            indexer.index(vtRecord, vtagsToIndex);
        } finally {
            if (lockObtained) {
                indexLocker.unlockLogFailure(recordId);
            }
        }
    }

}
