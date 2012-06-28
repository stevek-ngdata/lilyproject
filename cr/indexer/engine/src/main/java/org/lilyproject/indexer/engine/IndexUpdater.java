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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.indexer.model.indexerconf.DerefValue;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexField;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
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
import org.lilyproject.util.ObjectUtils;
import org.lilyproject.util.repo.RecordEvent;
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
    private LinkIndex linkIndex;
    private Indexer indexer;
    private IndexUpdaterMetrics metrics;
    private ClassLoader myContextClassLoader;
    private IndexLocker indexLocker;
    private RowLog rowLog;

    private Log log = LogFactory.getLog(getClass());
    private IdGenerator idGenerator;

    /**
     * @param rowLog this should be the message queue
     */
    public IndexUpdater(Indexer indexer, Repository repository,
                        LinkIndex linkIndex, IndexLocker indexLocker, RowLog rowLog, IndexUpdaterMetrics metrics)
            throws RowLogException, IOException {
        this.indexer = indexer;
        this.repository = repository;
        this.idGenerator = repository.getIdGenerator();
        this.linkIndex = linkIndex;
        this.indexLocker = indexLocker;
        this.rowLog = rowLog;

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
                boolean forUs = indexer.getIndexName().equals(event.getIndexName());

                if (forUs) {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Record %1$s: reindex requested for these vtags: %2$s", recordId,
                                indexer.vtagSetToNameString(event.getVtagsToIndex())));
                    }

                    index(recordId, event.getVtagsToIndex());
                }
            } else if (event.getType().equals(DELETE)) {
                // For deleted records, we cannot determine the record type, so we do not know if there was
                // an applicable index case, so we always perform a delete.
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
                updateDenormalizedData(recordId, event, null, null, null);
            } else {
                VTaggedRecord vtRecord;

                indexLocker.lock(recordId);
                try {
                    try {
                        // Read the vtags of the record. Note that while this algorithm is running, the record can
                        // meanwhile undergo changes. However, we continuously work with the snapshot of the vtags
                        // mappings read here. The processing of later events will bring the index up to date with
                        // any new changes.
                        vtRecord = new VTaggedRecord(recordId, event, null, repository);
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

                updateDenormalizedData(recordId, event, vtRecord.getUpdatedFieldsByScope(),
                        vtRecord.getVTagsByVersion(),
                        vtRecord.getModifiedVTags());
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
        Map<Scope, Set<FieldType>> updatedFieldsByScope = vtRecord.getUpdatedFieldsByScope();
        Map<Long, Set<SchemaId>> vtagsByVersion = vtRecord.getVTagsByVersion();

        // Determine the IndexCase:
        //  The indexing of all versions is determined by the record type of the non-versioned scope.
        //  This makes that the indexing behavior of all versions is equal, and can be changed (the
        //  record type of the versioned scope is immutable).
        IndexCase indexCase = indexer.getConf().getIndexCase(vtRecord.getRecord());

        if (indexCase == null) {
            // The record should not be indexed
            // But data from this record might be denormalized into other index entries
            // After this we go to update denormalized data
            if (log.isDebugEnabled()) {
                log.debug(String.format("Record %1$s: no index case found, record will not be indexed.",
                        vtRecord.getId()));
            }
        } else {
            Set<SchemaId> vtagsToIndex = new HashSet<SchemaId>();

            if (event.getType().equals(CREATE)) {
                // New record: just index everything
                indexer.setIndexAllVTags(vtagsToIndex, indexCase, vtRecord);
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
                indexer.setIndexAllVTags(vtagsToIndex, indexCase, vtRecord);

                // After this we go to the indexing
            } else { // a normal update

                //
                // Handle changes to non-versioned fields
                //
                if (updatedFieldsByScope.get(Scope.NON_VERSIONED).size() > 0) {
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.NON_VERSIONED))) {
                        indexer.setIndexAllVTags(vtagsToIndex, indexCase, vtRecord);
                        // After this we go to the treatment of changed vtag fields
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    String.format("Record %1$s: non-versioned fields changed, will reindex all vtags.",
                                            vtRecord.getId()));
                        }
                    }
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
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED))
                            || atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE))) {

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
                Set<SchemaId> changedVTagFields = vtRecord.getModifiedVTags();
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

    private void updateDenormalizedData(RecordId recordId, RecordEvent event,
                                        Map<Scope, Set<FieldType>> updatedFieldsByScope,
                                        Map<Long, Set<SchemaId>> vtagsByVersion,
                                        Set<SchemaId> changedVTagFields)
            throws RepositoryException, InterruptedException, LinkIndexException {

        // This algorithm is designed to first collect all the reindex-work, and then to perform it.
        // Otherwise the same document would be indexed multiple times if it would become invalid
        // because of different reasons (= different indexFields).

        //
        // Collect all the relevant IndexFields, and for each the relevant vtags
        //

        // This map will contain all the IndexFields we need to treat, and for each one the vtags to be considered
        Map<IndexField, Set<SchemaId>> indexFieldsAndVTags = new IdentityHashMap<IndexField, Set<SchemaId>>() {
            @Override
            public Set<SchemaId> get(Object key) {
                if (!this.containsKey(key) && key instanceof IndexField) {
                    this.put((IndexField) key, new HashSet<SchemaId>());
                }
                return super.get(key);
            }
        };

        // There are two cases when denormalized data needs updating:
        //   1. when the content of a (vtagged) record changes
        //   2. when vtags change (are added, removed or point to a different version)
        // We now handle these 2 cases.

        // === Case 1 === updates in response to changes to this record
        findRelevantIndexFieldsForRecordChanges(event, updatedFieldsByScope, vtagsByVersion, indexFieldsAndVTags);

        // === Case 2 === handle updated/added/removed vtags
        findRelevantIndexFieldsForVTagChanges(changedVTagFields, indexFieldsAndVTags);

        //
        // Now search the referrers, that is: for each link field, find out which records point to the current record
        // in a certain versioned view (= a certain vtag)
        //

        // This map holds the referrer records to reindex, and for which versions (vtags) they need to be reindexed.
        Map<RecordId, Set<SchemaId>> referrersAndVTags = new HashMap<RecordId, Set<SchemaId>>() {
            @Override
            public Set<SchemaId> get(Object key) {
                if (!containsKey(key) && key instanceof RecordId) {
                    put((RecordId) key, new HashSet<SchemaId>());
                }
                return super.get(key);
            }
        };

        int searchedFollowCount = 0;

        // Run over the IndexFields
        for (Map.Entry<IndexField, Set<SchemaId>> entry : indexFieldsAndVTags.entrySet()) {
            IndexField indexField = entry.getKey();
            Set<SchemaId> referrerVTags = entry.getValue();
            DerefValue derefValue = (DerefValue) indexField.getValue();

            // Run over the version tags
            for (SchemaId referrerVtag : referrerVTags) {
                List<DerefValue.Follow> follows = derefValue.getCrossRecordFollows();

                Set<RecordId> referrers = new HashSet<RecordId>();
                referrers.add(recordId);

                for (int i = follows.size() - 1; i >= 0; i--) {
                    searchedFollowCount++;
                    DerefValue.Follow follow = follows.get(i);

                    Set<RecordId> newReferrers = new HashSet<RecordId>();

                    if (follow instanceof DerefValue.LinkFieldFollow) {
                        final DerefValue.LinkFieldFollow linkFollowField = (DerefValue.LinkFieldFollow) follow;
                        newReferrers.addAll(searchReferrersLinkFieldFollow(referrerVtag, referrers, linkFollowField));
                    } else if (follow instanceof DerefValue.VariantFollow) {
                        DerefValue.VariantFollow varFollow = (DerefValue.VariantFollow) follow;
                        newReferrers.addAll(searchReferrersVariantFollow(referrers, varFollow));
                    } else if (follow instanceof DerefValue.ForwardVariantFollow) {
                        final DerefValue.ForwardVariantFollow forwardVarFollow =
                                (DerefValue.ForwardVariantFollow) follow;
                        newReferrers.addAll(searchReferrersForwardVariantFollow(referrers, forwardVarFollow));
                    } else if (follow instanceof DerefValue.MasterFollow) {
                        newReferrers.addAll(searchReferrersMasterFollow(referrers));
                    } else {
                        throw new RuntimeException("Unexpected implementation of DerefValue.Follow: " +
                                follow.getClass().getName());
                    }

                    referrers = newReferrers;
                }

                for (RecordId referrer : referrers) {
                    referrersAndVTags.get(referrer).add(referrerVtag);
                }
            }
        }


        if (log.isDebugEnabled()) {
            log.debug(String.format("Record %1$s: found %2$s records (times vtags) to be updated because they " +
                    "might contain outdated denormalized data. Checked %3$s follow instances." +
                    " The records are: %4$s", recordId,
                    referrersAndVTags.size(), searchedFollowCount, referrersAndVTags.keySet()));
        }

        //
        // Now add an index message to each of the found referrers, their actual indexing
        // will be triggered by the message queue.
        //
        for (Map.Entry<RecordId, Set<SchemaId>> entry : referrersAndVTags.entrySet()) {
            RecordId referrer = entry.getKey();
            Set<SchemaId> vtagsToIndex = entry.getValue();

            if (vtagsToIndex.isEmpty()) {
                continue;
            }

            RecordEvent payload = new RecordEvent();
            payload.setType(INDEX);
            payload.setIndexName(indexer.getIndexName());
            for (SchemaId vtag : vtagsToIndex) {
                payload.addVTagToIndex(vtag);
            }

            // TODO how will this behave if the row was meanwhile deleted?
            try {
                rowLog.putMessage(referrer.toBytes(), null, payload.toJsonBytes(), null);
            } catch (Exception e) {
                // We failed to put the message: this is pretty important since it means the record's index
                // won't get updated, therefore log as error, but after this we continue with the next one.
                log.error("Error putting index message on queue of record " + referrer, e);
                metrics.errors.inc();
            }
        }
    }

    private void findRelevantIndexFieldsForRecordChanges(RecordEvent event,
                                                         Map<Scope, Set<FieldType>> updatedFieldsByScope,
                                                         Map<Long, Set<SchemaId>> vtagsByVersion,
                                                         Map<IndexField, Set<SchemaId>> indexFieldsAndVTags) {
        long version = event.getVersionCreated() == -1 ? event.getVersionUpdated() : event.getVersionCreated();

        // Determine the relevant index fields
        List<IndexField> indexFields;
        if (event.getType() == RecordEvent.Type.DELETE) {
            indexFields = indexer.getConf().getDerefIndexFields();
        } else {
            indexFields = new ArrayList<IndexField>();

            collectDerefIndexFields(updatedFieldsByScope.get(Scope.NON_VERSIONED), indexFields);

            if (version != -1 && vtagsByVersion.get(version) != null) {
                collectDerefIndexFields(updatedFieldsByScope.get(Scope.VERSIONED), indexFields);
                collectDerefIndexFields(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE), indexFields);
            }
        }

        // For each indexField, determine the vtags of the referrer that we should consider.
        // In the context of this algorithm, a referrer is each record whose index might contain
        // denormalized data from the record of which we are now processing the change event.
        for (IndexField indexField : indexFields) {
            DerefValue derefValue = (DerefValue) indexField.getValue();
            FieldType fieldType = derefValue.getLastRealField();

            //
            // Determine the vtags of the referrer that we should consider
            //
            Set<SchemaId> referrerVtags = indexFieldsAndVTags.get(indexField);

            if (fieldType.getScope() == Scope.NON_VERSIONED || event.getType() == RecordEvent.Type.DELETE) {
                // If it is a non-versioned field, then all vtags should be considered.
                // If it is a delete event, we do not know what vtags existed for the record, so consider them all.
                referrerVtags.addAll(indexer.getConf().getVtags());
            } else {
                // Otherwise only the vtags of the created/updated version, if any
                if (version != -1) {
                    Set<SchemaId> vtags = vtagsByVersion.get(version);
                    if (vtags != null)
                        referrerVtags.addAll(vtags);
                }
            }
        }
    }

    private void findRelevantIndexFieldsForVTagChanges(Set<SchemaId> changedVTagFields,
                                                       Map<IndexField, Set<SchemaId>> indexFieldsAndVTags) {
        if (changedVTagFields != null && !changedVTagFields.isEmpty()) {
            // In this case, the IndexFields which we need to handle are those that use fields from:
            //  - the previous version to which the vtag pointed (if it is not a new vtag)
            //  - the new version to which the vtag points (if it is not a deleted vtag)
            // But rather than calculating all that (consider the need to retrieve the versions),
            // for now we simply consider all IndexFields.
            // TODO could optimize this to exclude deref fields that use only non-versioned fields?
            for (IndexField indexField : indexer.getConf().getDerefIndexFields()) {
                indexFieldsAndVTags.get(indexField).addAll(changedVTagFields);
            }
        }
    }

    private Set<RecordId> searchReferrersLinkFieldFollow(SchemaId referrerVtag, Set<RecordId> referrers,
                                                         DerefValue.LinkFieldFollow linkFollowField)
            throws LinkIndexException {
        final SchemaId fieldId = linkFollowField.getOwnerFieldType().getId();

        final HashSet<RecordId> result = new HashSet<RecordId>();
        for (RecordId referrer : referrers) {
            Set<RecordId> linkReferrers = linkIndex.getReferrers(referrer, referrerVtag, fieldId);
            result.addAll(linkReferrers);
        }
        return result;
    }

    private Set<RecordId> searchReferrersVariantFollow(Set<RecordId> referrers, DerefValue.VariantFollow varFollow)
            throws RepositoryException, InterruptedException {
        final HashSet<RecordId> result = new HashSet<RecordId>();

        Set<String> dimensions = varFollow.getDimensions();

        // We need to find out the variants of the current set of referrers which have the
        // same variant properties as the referrer (= same key/value pairs) and additionally
        // have the extra dimensions defined in the VariantFollow.

        nextReferrer:
        for (RecordId referrer : referrers) {

            Map<String, String> refprops = referrer.getVariantProperties();

            // If the referrer already has one of the dimensions, then skip it
            for (String dimension : dimensions) {
                if (refprops.containsKey(dimension))
                    continue nextReferrer;
            }

            //
            Set<RecordId> variants = repository.getVariants(referrer);

            nextVariant:
            for (RecordId variant : variants) {
                Map<String, String> varprops = variant.getVariantProperties();

                // Check it has the same value for each of the variant properties of the current referrer record
                for (Map.Entry<String, String> refprop : refprops.entrySet()) {
                    if (!ObjectUtils.safeEquals(varprops.get(refprop.getKey()), refprop.getValue())) {
                        // skip this variant
                        continue nextVariant;
                    }
                }

                // Check it has the additional dimensions
                for (String dimension : dimensions) {
                    if (!varprops.containsKey(dimension))
                        continue nextVariant;
                }

                // We have a hit
                result.add(variant);
            }
        }

        return result;
    }

    private Set<RecordId> searchReferrersForwardVariantFollow(Set<RecordId> referrers,
                                                              DerefValue.ForwardVariantFollow forwardVarFollow)
            throws RepositoryException, InterruptedException {
        final HashSet<RecordId> result = new HashSet<RecordId>();

        // If the current referrer has the variant dimensions which were specified in the
        // ForwardVariantFollow, we need to reindex all records which have the same RecordId but without
        // one of the given variant dimensions.

        for (RecordId referrer : referrers) {

            // If the referrer does not have all the dimensions with the correct values, skip it
            if (hasAllDefinedDimensions(referrer, forwardVarFollow) &&
                    hasSameValueForValuedDimensions(referrer, forwardVarFollow)) {

                // start with all variants of the current referrer and find the ones which have the same properties
                // except for the given variant dimensions of the ForwardVariantFollow
                final Set<RecordId> variants = repository.getVariants(referrer.getMaster());

                for (RecordId variant : variants) {
                    Map<String, String> variantProps = variant.getVariantProperties();

                    // Check it doesn't have all the given dimensions
                    if (!haveAllDefinedDimensions(variantProps, forwardVarFollow)) {

                        boolean allVariantsEqualExceptGivenDimension = true;

                        // Check it has the same value for each of the variant properties of the current referrer
                        // record, except for the dimensions from the ForwardVariantFollow
                        Map<String, String> referrerVariantProps = referrer.getVariantProperties();
                        for (Map.Entry<String, String> referrerProp : referrerVariantProps.entrySet()) {
                            if (!forwardVarFollow.getDimensions()
                                    .containsKey(referrerProp.getKey())) { // ignore given dimensions
                                if (!ObjectUtils.safeEquals(variantProps.get(referrerProp.getKey()),
                                        referrerProp.getValue())) {
                                    allVariantsEqualExceptGivenDimension = false;
                                    break;
                                }
                            }
                        }

                        if (allVariantsEqualExceptGivenDimension) // We have a hit
                            result.add(variant);
                    }
                }
            }
        }

        return result;
    }

    private boolean hasAllDefinedDimensions(RecordId referrer, DerefValue.ForwardVariantFollow forwardVarFollow) {
        return haveAllDefinedDimensions(referrer.getVariantProperties(), forwardVarFollow);
    }

    private boolean haveAllDefinedDimensions(Map<String, String> variantProperties,
                                             DerefValue.ForwardVariantFollow forwardVarFollow) {
        return variantProperties.keySet().containsAll(forwardVarFollow.getDimensions().keySet());
    }

    private boolean hasSameValueForValuedDimensions(RecordId referrer,
                                                    DerefValue.ForwardVariantFollow forwardVarFollow) {
        return referrer.getVariantProperties().entrySet()
                .containsAll(forwardVarFollow.getValuedDimensions().entrySet());
    }

    private Set<RecordId> searchReferrersMasterFollow(Set<RecordId> referrers)
            throws RepositoryException, InterruptedException {
        final HashSet<RecordId> result = new HashSet<RecordId>();

        for (RecordId referrer : referrers) {
            // A MasterFollow can only point to masters
            if (referrer.isMaster()) {
                Set<RecordId> variants = repository.getVariants(referrer);
                variants.remove(referrer);
                result.addAll(variants);
            }
        }
        return result;
    }

    /**
     * Index a record for all the specified vtags.
     */
    private void index(RecordId recordId, Set<SchemaId> vtagsToIndex) throws RepositoryException, InterruptedException,
            SolrClientException, ShardSelectorException, IndexLockException {
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

    private void collectDerefIndexFields(Set<FieldType> fieldTypes, List<IndexField> indexFields) {
        for (FieldType fieldType : fieldTypes) {
            indexFields.addAll(indexer.getConf().getDerefIndexFields(fieldType.getId()));
        }
    }

    private boolean atLeastOneUsedInIndex(Set<FieldType> fieldTypes) {
        for (FieldType type : fieldTypes) {
            if (indexer.getConf().isIndexFieldDependency(type)) {
                return true;
            }
        }
        return false;
    }
}
