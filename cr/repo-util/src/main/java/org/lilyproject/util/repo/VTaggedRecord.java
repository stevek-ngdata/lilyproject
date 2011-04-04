package org.lilyproject.util.repo;

import org.lilyproject.repository.api.*;

import java.util.*;

import static org.lilyproject.util.repo.RecordEvent.Type.CREATE;

/**
 * This is a record with added logic/state for version tag behavior.
 */
public class VTaggedRecord {

    /**
     * The record object containing only the non-versioned fields.
     */
    private IdRecord record;

    private Map<SchemaId, Long> vtags;

    private Map<Long, Set<SchemaId>> tagsByVersion;

    private RecordEvent recordEvent;

    private SchemaId lastVTag;

    private Repository repository;

    private TypeManager typeManager;

    private Map<Scope, Set<FieldType>> updatedFieldsByScope;

    private FieldFilter fieldFilter;

    public VTaggedRecord(RecordId recordId, Repository repository) throws RepositoryException, InterruptedException {
        this(recordId, null, repository);
    }

    public VTaggedRecord(RecordId recordId, FieldFilter fieldFilter, Repository repository) throws RepositoryException, InterruptedException {
        this(recordId, null, fieldFilter, repository);
    }

    public VTaggedRecord(RecordId recordId, RecordEvent recordEvent, FieldFilter fieldFilter, Repository repository)
            throws RepositoryException, InterruptedException {

        this.repository = repository;
        typeManager = repository.getTypeManager();

        // Load the last version of the record to get vtag and non-versioned fields information
        record = repository.readWithIds(recordId, null, null);
        reduceToNonVersioned(record, typeManager);

        this.recordEvent = recordEvent;
        this.fieldFilter = fieldFilter != null ? fieldFilter : PASS_ALL_FIELD_FILTER;
    }

    public IdRecord getNonVersionedRecord() {
        return record;
    }


    public SchemaId getLastVTag() throws RepositoryException, InterruptedException {
        if (lastVTag == null) {
            lastVTag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();
        }
        return lastVTag;
    }

    /**
     * The set of vtags defined on the record, including the last vtag.
     *
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
            } catch (RepositoryException e) {
                // Other problem loading field type: skip it
                // TODO log this also as an error
                continue;
            }

            if (VersionTag.isVersionTag(fieldType)) {
                vtags.put(fieldType.getId(), (Long)field.getValue());
            }
        }

        vtags.put(getLastVTag(), record.getVersion() == null ? 0 : record.getVersion());

        return vtags;
    }

    public RecordEvent getRecordEvent() {
        return recordEvent;
    }

    public Set<SchemaId> getModifiedVTags() throws RepositoryException, InterruptedException {
        Set<SchemaId> changedVTags = VersionTag.filterVTagFields(recordEvent.getUpdatedFields(), typeManager);

        // Last vtag
        if (recordEvent.getVersionCreated() != -1 || recordEvent.getType() == CREATE) {
            changedVTags.add(getLastVTag());
        }

        return changedVTags;
    }

    public Set<SchemaId> getVTagsOfModifiedData() throws RepositoryException, InterruptedException {
        Set<SchemaId> vtagsOfChangedData = null;

        // Make sure these are calculated
        getUpdatedFieldsByScope();
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

    public Map<Scope, Set<FieldType>> getUpdatedFieldsByScope() throws RepositoryException, InterruptedException {
        if (updatedFieldsByScope == null) {
            updatedFieldsByScope = getFieldTypeAndScope(recordEvent.getUpdatedFields());
        }
        return updatedFieldsByScope;
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
        } else if (version == 0L) {
            return getNonVersionedRecord();
        } else {
            return repository.readWithIds(record.getId(), version, fields);
        }
    }

    public IdRecord getIdRecord(long version) throws InterruptedException, RepositoryException {
        if (version == 0L) {
            return getNonVersionedRecord();
        } else {
            return repository.readWithIds(record.getId(), version, null);
        }
    }

    /**
     * Removes any versioned information from the supplied record object.
     *
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
        record.setRecordType(Scope.VERSIONED, null, null);
        record.setRecordType(Scope.VERSIONED_MUTABLE, null, null);
    }

    private Map<Scope, Set<FieldType>> getFieldTypeAndScope(Set<SchemaId> fieldIds)
            throws RepositoryException, InterruptedException {

        Map<Scope, Set<FieldType>> result = new HashMap<Scope, Set<FieldType>>();
        for (Scope scope : Scope.values()) {
            result.put(scope, new HashSet<FieldType>());
        }

        for (SchemaId fieldId : fieldIds) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeById(fieldId);
            } catch (FieldTypeNotFoundException e) {
                continue;
            }
            if (fieldFilter.accept(fieldType)) {
                result.get(fieldType.getScope()).add(fieldType);
            }
        }

        return result;
    }

    public static interface FieldFilter {
        boolean accept(FieldType fieldtype);
    }

    private static final FieldFilter PASS_ALL_FIELD_FILTER = new FieldFilter() {
        @Override
        public boolean accept(FieldType fieldtype) {
            return true;
        }
    };
}
