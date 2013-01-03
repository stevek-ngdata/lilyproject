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
package org.lilyproject.util.repo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;

/**
 * This is a record with added logic/state for version tag behavior.
 */
public class VTaggedRecord {

    /**
     * The record containing the last version (or none if non-versioned fields only).
     */
    private final IdRecord record;

    /**
     * The record object containing only the non-versioned fields.
     */
    private IdRecord nonVersionedRecord;

    private Map<SchemaId, Long> vtags;

    private Map<Long, Set<SchemaId>> tagsByVersion;

    private RecordEvent recordEvent;

    private RecordEventHelper recordEventHelper;

    private SchemaId lastVTag;

    private Repository repository;

    private TypeManager typeManager;

    public VTaggedRecord(RecordId recordId, Repository repository) throws RepositoryException, InterruptedException {
        this(recordId, null, repository);
    }

    /**
     * Construct based on a recordId. This will do a repository read to get the latest record.
     */
    public VTaggedRecord(RecordId recordId, RecordEventHelper eventHelper, Repository repository)
            throws RepositoryException, InterruptedException {
        // Load the last version of the record to get vtag and non-versioned fields information
        // We will also reuse this record object in case the last version or the non-versioned data is needed,
        // to avoid extra gets on HBase.
        this(repository.readWithIds(recordId, null, null), eventHelper, repository);
    }

    /**
     * Construct based on an existing record to prevent additional repository reads when the record is already
     * available. The existing IdRecord should be the last (when it was read) and should have been read with all
     * fields!
     */
    public VTaggedRecord(IdRecord idRecord, RecordEventHelper eventHelper, Repository repository)
            throws RepositoryException, InterruptedException {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.record = idRecord;
        this.recordEventHelper = eventHelper;
        if (eventHelper != null) {
            this.recordEvent = eventHelper.getEvent();
        }
    }

    public RecordId getId() {
        return record.getId();
    }

    /**
     * Returns the record object of the last version of the record, or the non-versioned record object if the
     * record has no versions.
     */
    public IdRecord getRecord() {
        return record;
    }

    public IdRecord getNonVersionedRecord() throws RepositoryException, InterruptedException {
        if (nonVersionedRecord == null) {
            if (record.getVersion() == null) {
                // record has no version, so no versioned fields, so no cloning necessary
                this.nonVersionedRecord = record;
            } else {
                IdRecord nonVersionedRecord = record.cloneRecord();
                reduceToNonVersioned(nonVersionedRecord, typeManager);
                this.nonVersionedRecord = nonVersionedRecord;
            }
        }
        return nonVersionedRecord;
    }


    public SchemaId getLastVTag() throws RepositoryException, InterruptedException {
        if (lastVTag == null) {
            lastVTag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();
        }
        return lastVTag;
    }

    /**
     * The set of vtags defined on the record, including the last vtag.
     * <p/>
     * <p>Note that version numbers do not necessarily correspond to existing versions, a user might
     * have defined invalid vtags.
     */
    public Map<SchemaId, Long> getVTags() throws InterruptedException, RepositoryException {
        if (vtags == null) {
            vtags = getTagsById(record, typeManager);
        }

        return vtags;
    }

    private Map<SchemaId, Long> getTagsById(IdRecord record, TypeManager typeManager)
            throws InterruptedException, RepositoryException {

        Map<SchemaId, Long> vtags = new HashMap<SchemaId, Long>();

        for (Map.Entry<SchemaId, Object> field : record.getFieldsById().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeById(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // A field whose field type does not exist: skip it
                continue;
            }

            if (VersionTag.isVersionTag(fieldType)) {
                vtags.put(fieldType.getId(), (Long)field.getValue());
            }
        }

        vtags.put(getLastVTag(), record.getVersion() == null ? new Long(0) : record.getVersion());

        return vtags;
    }

    public RecordEvent getRecordEvent() {
        return recordEvent;
    }

    public RecordEventHelper getRecordEventHelper() {
        return recordEventHelper;
    }

    public Set<SchemaId> getVTagsOfModifiedData() throws RepositoryException, InterruptedException {
        Set<SchemaId> vtagsOfChangedData = null;

        Map<Scope, Set<FieldType>> updatedFieldsByScope = recordEventHelper.getUpdatedFieldsByScope();

        // Make sure these are calculated
        getVTags();
        getVTagsByVersion();

        // If non-versioned fields changed: all vtags are affected since each vtag-based view also includes the
        // non-versioned fields
        // Note that the updated fields also include deleted fields, as it should.
        if (!updatedFieldsByScope.get(Scope.NON_VERSIONED).isEmpty()) {
            vtagsOfChangedData = vtags.keySet();
        } else if (!updatedFieldsByScope.get(Scope.VERSIONED).isEmpty() ||
                !updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE).isEmpty()) {

            if (recordEvent.getVersionCreated() != -1) {
                vtagsOfChangedData = tagsByVersion.get(recordEvent.getVersionCreated());
            } else if (recordEvent.getVersionUpdated() != -1) {
                vtagsOfChangedData = tagsByVersion.get(recordEvent.getVersionUpdated());
            }
        }

        return vtagsOfChangedData != null ? vtagsOfChangedData : Collections.<SchemaId>emptySet();
    }

    public Map<Long, Set<SchemaId>> getVTagsByVersion() throws InterruptedException, RepositoryException {
        if (tagsByVersion == null) {
            tagsByVersion = idTagsByVersion(getVTags());
        }
        return tagsByVersion;
    }

    /**
     * Inverts a map containing version by tag to a map containing id tags by version.
     */
    private Map<Long, Set<SchemaId>> idTagsByVersion(Map<SchemaId, Long> vtags) {
        Map<Long, Set<SchemaId>> result = new HashMap<Long, Set<SchemaId>>();

        for (Map.Entry<SchemaId, Long> entry : vtags.entrySet()) {
            Set<SchemaId> tags = result.get(entry.getValue());
            if (tags == null) {
                tags = new HashSet<SchemaId>();
                result.put(entry.getValue(), tags);
            }
            tags.add(entry.getKey());
        }

        return result;
    }

    public IdRecord getIdRecord(SchemaId vtagId) throws InterruptedException, RepositoryException {
        return getIdRecord(vtagId, null);
    }

    public IdRecord getIdRecord(SchemaId vtagId, List<SchemaId> fields) throws InterruptedException,
            RepositoryException {

        Long version = getVTags().get(vtagId);
        if (version == null) {
            return null;
        }

        return getIdRecord(version, fields);
    }

    public IdRecord getIdRecord(long version) throws InterruptedException, RepositoryException {
        return getIdRecord(version, null);
    }

    private IdRecord getIdRecord(long version, List<SchemaId> fields) throws InterruptedException, RepositoryException {
        // TODO in case of the cached copies, we should filter the fields to the requested fields (not used anywhere
        // at the time of this writing)
        if (version == 0L) {
            return getNonVersionedRecord();
        } else if (record.getVersion() != null && version == record.getVersion()) {
            return record;
        } else {
            return repository.readWithIds(record.getId(), version, fields);
        }
    }

    /**
     * Removes any versioned information from the supplied record object.
     * <p/>
     * <p>This method can be removed once we have a repository method that is able to filter this when loading
     * the record.
     */
    public static void reduceToNonVersioned(IdRecord record, TypeManager typeManager)
            throws RepositoryException, InterruptedException {

        if (record.getVersion() == null) {
            // The record has no versions so there should be no versioned fields in it
            return;
        }

        // Remove all non-versioned fields from the record
        Map<SchemaId, QName> mapping = record.getFieldIdToNameMapping();
        Iterator<Map.Entry<SchemaId, QName>> it = mapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<SchemaId, QName> entry = it.next();
            if (typeManager.getFieldTypeById(entry.getKey()).getScope() != Scope.NON_VERSIONED) {
                record.delete(entry.getValue(), false);
                it.remove();
            }
        }

        // Remove versioned record type info
        record.setRecordType(Scope.VERSIONED, (QName)null, null);
        record.setRecordType(Scope.VERSIONED_MUTABLE, (QName)null, null);
    }
}
