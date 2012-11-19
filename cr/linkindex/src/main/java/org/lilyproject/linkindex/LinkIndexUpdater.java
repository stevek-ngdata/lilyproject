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
package org.lilyproject.linkindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.linkindex.LinkIndexUpdaterMetrics.Action;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.FieldFilter;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEventHelper;
import org.lilyproject.util.repo.VTaggedRecord;

import java.util.*;

import static org.lilyproject.util.repo.RecordEvent.Type.*;

// TODO think more about error processing:
//      Some kinds of errors might be temporary in nature and be solved by retrying after some time.
//      This would seem preferable rather than just giving up and leaving the link index in an incorrect state.
//      Also to consider: if an error occurs, then delete all links for the record and or vtag, rather than just
//      logging the error

/**
 * Keeps the {@link LinkIndex} up to date when changes happen to records.
 */
// FIXME ROWLOG REFACTORING
public class LinkIndexUpdater {}
//implements RowLogMessageListener {
//    private Repository repository;
//    private TypeManager typeManager;
//    private LinkIndex linkIndex;
//
//    private Log log = LogFactory.getLog(getClass());
//    private LinkIndexUpdaterMetrics metrics;
//
//    public LinkIndexUpdater(Repository repository, LinkIndex linkIndex) throws RepositoryException, InterruptedException {
//        this.repository = repository;
//        this.typeManager = repository.getTypeManager();
//        this.linkIndex = linkIndex;
//        metrics = new LinkIndexUpdaterMetrics("linkIndexUpdater");
//    }
//
//    @Override
//    public boolean processMessage(RowLogMessage msg) {
//        try {
//            RecordId recordId = repository.getIdGenerator().fromBytes(msg.getRowKey());
//            Object context = msg.getContext();
//            RecordEvent recordEvent = null;
//            if (context != null) {
//                RowLogContext rowLogContext = (RowLogContext) msg.getContext();
//                recordEvent = rowLogContext.getRecordEvent();
//            }
//            if (recordEvent == null)
//                recordEvent = new RecordEvent(msg.getPayload(), repository.getIdGenerator());
//            update(recordId, recordEvent);
//        } catch (Exception e) {
//            log.error("Error processing event in LinkIndexUpdater", e);
//        }
//        return true;
//    }
//
//    public void update(RecordId recordId, RecordEvent recordEvent) {
//        // This is the algorithm for updating the LinkIndex when a record changes.
//        //
//        // The LinkIndex contains, for each vtag defined on the record, the links extracted from the record
//        // in that version. If the record has no vtags, there will hence be no entries in the link index.
//        // However, each record has the implicit 'last' vtag, so it will at least contain the links extracted
//        // for that vtag.
//        //
//        // There are basically two kinds of changes that require updating the link index:
//        //  * the content of (non-vtag) fields is changed
//        //  * the vtags change: existing vtag now points to another version, a new vtag is added, or a vtag is removed
//        //
//
//        long before = System.currentTimeMillis();
//        try {
//            if (recordEvent.getType().equals(DELETE)) {
//                // Delete everything from the link index for this record, thus for all vtags
//                linkIndex.deleteLinks(recordId);
//                if (log.isDebugEnabled()) {
//                    log.debug("Record " + recordId + " : delete event : deleted extracted links.");
//                }
//            } else if (recordEvent.getType().equals(CREATE) || recordEvent.getType().equals(UPDATE)) {
//                boolean isNewRecord = recordEvent.getType().equals(CREATE);
//
//                RecordEventHelper eventHelper = new RecordEventHelper(recordEvent, LINK_FIELD_FILTER,
//                        repository.getTypeManager());
//
//                VTaggedRecord vtRecord;
//                try {
//                    vtRecord = new VTaggedRecord(recordId, eventHelper, repository);
//                } catch (RecordNotFoundException e) {
//                    // record not found: delete all links for all vtags
//                    linkIndex.deleteLinks(recordId);
//                    if (log.isDebugEnabled()) {
//                        log.debug("Record " + recordId + " : does not exist : deleted extracted links.");
//                    }
//                    return;
//                }
//
//                //
//                // First find out for what vtags we need to re-perform the link extraction
//                //
//                Set<SchemaId> vtagsToProcess = new HashSet<SchemaId>();
//
//                // Modified vtag fields
//                vtagsToProcess.addAll(eventHelper.getModifiedVTags());
//
//                // The vtags of the created/modified version, if any, and if any link fields changed
//                vtagsToProcess.addAll(vtRecord.getVTagsOfModifiedData());
//
//                Map<SchemaId, Long> vtags = vtRecord.getVTags();
//
//                //
//                // For each of the vtags, perform the link extraction
//                //
//                Map<Long, Set<FieldedLink>> cache = new HashMap<Long, Set<FieldedLink>>();
//                for (SchemaId vtag : vtagsToProcess) {
//                    if (!vtags.containsKey(vtag)) {
//                        // The vtag is not defined on the document: it is a deleted vtag, delete the
//                        // links corresponding to it
//                        linkIndex.deleteLinks(recordId, vtag);
//                        if (log.isDebugEnabled()) {
//                            log.debug(String.format("Record %1$s, vtag %2$s : deleted extracted links " +
//                                    "because vtag does not exist on document anymore",
//                                    recordId, safeLoadTagName(vtag)));
//                        }
//                    } else {
//                        // Since one version might have multiple vtags, we keep a little cache to avoid
//                        // extracting the links from the same version twice.
//                        long version = vtags.get(vtag);
//                        Set<FieldedLink> links;
//                        if (cache.containsKey(version)) {
//                            links = cache.get(version);
//                        } else {
//                            links = extractLinks(vtRecord, version);
//                            cache.put(version, links);
//                        }
//                        linkIndex.updateLinks(recordId, vtag, links, isNewRecord);
//                        if (log.isDebugEnabled()) {
//                            log.debug(String.format("Record %1$s, vtag %2$s : extracted links count : %3$s",
//                                    recordId, safeLoadTagName(vtag), links.size()));
//                        }
//                    }
//                }
//            }
//        } catch (Exception e) {
//            log.error("Error processing event in LinkIndexUpdater", e);
//        } finally {
//            metrics.report(Action.UPDATE, System.currentTimeMillis() - before);
//        }
//    }
//
//    private Set<FieldedLink> extractLinks(VTaggedRecord vtRecord, Long version) {
//        long before = System.currentTimeMillis();
//        try {
//            Set<FieldedLink> links;
//            IdRecord versionRecord = null;
//            try {
//                versionRecord = vtRecord.getIdRecord(version);
//            } catch (RecordNotFoundException e) {
//                // vtag points to a non-existing record
//            }
//
//            if (versionRecord == null) {
//                links = Collections.emptySet();
//            } else {
//                LinkCollector collector = new LinkCollector();
//                RecordLinkExtractor.extract(versionRecord, collector, repository);
//                links = collector.getLinks();
//            }
//            return links;
//        } catch (VersionNotFoundException e) {
//            // A vtag pointing to a non-existing version, nothing unusual.
//            return Collections.emptySet();
//        } catch (Throwable t) {
//            log.error("Error extracting links from record " + vtRecord.getId(), t);
//        } finally {
//            metrics.report(Action.EXTRACT, System.currentTimeMillis() - before);
//        }
//        return Collections.emptySet();
//    }
//
//    /**
//     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
//     */
//    private String safeLoadTagName(SchemaId fieldTypeId) {
//        if (fieldTypeId == null)
//            return "null";
//
//        try {
//            return typeManager.getFieldTypeById(fieldTypeId).getName().getName();
//        } catch (Throwable t) {
//            return "failed to load name";
//        }
//    }
//
//    private static final FieldFilter LINK_FIELD_FILTER = new FieldFilter() {
//        @Override
//        public boolean accept(FieldType fieldtype) {
//            return fieldtype.getValueType().getDeepestValueType().getBaseName().equals("LINK");
//        }
//    };
//
//}
