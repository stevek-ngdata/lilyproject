package org.lilycms.tools.import_;

import org.lilycms.repository.api.*;

import java.util.*;

public class ImportTool {
    private Repository repository;
    private TypeManager typeManager;
    private ImportListener importListener;

    public ImportTool(Repository repository, ImportListener importListener) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.importListener = importListener;
    }

    public FieldType importFieldType(FieldType newFieldType) throws RepositoryException, ImportConflictException {
        FieldType oldFieldType = null;
        try {
            oldFieldType = typeManager.getFieldTypeByName(newFieldType.getName());
        } catch (FieldTypeNotFoundException e) {
            // ok
        }

        if (oldFieldType != null) {
            // check it is similar
            String oldPrimitive = oldFieldType.getValueType().getPrimitive().getName();
            String newPrimitive = newFieldType.getValueType().getPrimitive().getName();
            checkEquals(oldPrimitive, newPrimitive, EntityType.FIELD_TYPE, "primitive type", newFieldType.getName().toString());

            boolean oldMultivalue = oldFieldType.getValueType().isMultiValue();
            boolean newMultiValue = newFieldType.getValueType().isMultiValue();
            checkEquals(oldMultivalue, newMultiValue, EntityType.FIELD_TYPE, "multi-value", newFieldType.getName().toString());

            boolean oldHierarchical = oldFieldType.getValueType().isHierarchical();
            boolean newHierarchical = newFieldType.getValueType().isHierarchical();
            checkEquals(oldHierarchical, newHierarchical, EntityType.FIELD_TYPE, "hierarchical", newFieldType.getName().toString());

            Scope oldScope = oldFieldType.getScope();
            Scope newScope = newFieldType.getScope();
            checkEquals(oldScope, newScope, EntityType.FIELD_TYPE, "scope", newFieldType.getName().toString());

            // everything equal, skip it
            importListener.existsAndEqual(EntityType.FIELD_TYPE, newFieldType.getName().toString(), null);
            return oldFieldType;
        }

        FieldType createdFieldType = typeManager.createFieldType(newFieldType);
        importListener.created(EntityType.FIELD_TYPE, createdFieldType.getName().toString(), createdFieldType.getId());
        return createdFieldType;
    }

    public RecordType importRecordType(RecordType newRecordType) throws RepositoryException {
        RecordType oldRecordType = null;
        try {
            oldRecordType = typeManager.getRecordType(newRecordType.getId(), null);
        } catch (RecordTypeNotFoundException e) {
            // ok
        }

        if (oldRecordType != null) {
            // check it is similar
            Set<FieldTypeEntry> oldFieldTypeEntries = new HashSet<FieldTypeEntry>(oldRecordType.getFieldTypeEntries());
            Set<FieldTypeEntry> newFieldTypeEntries = new HashSet<FieldTypeEntry>(newRecordType.getFieldTypeEntries());
            boolean updated = false;
            if (!newFieldTypeEntries.equals(oldFieldTypeEntries)) {
                updated = true;
                // update the record type
                for (FieldTypeEntry entry : newFieldTypeEntries) {
                    if (oldRecordType.getFieldTypeEntry(entry.getFieldTypeId()) == null) {
                        oldRecordType.addFieldTypeEntry(entry);
                    }
                }
                for (FieldTypeEntry entry : oldFieldTypeEntries) {
                    if (newRecordType.getFieldTypeEntry(entry.getFieldTypeId()) == null) {
                        oldRecordType.removeFieldTypeEntry(entry.getFieldTypeId());
                    }
                }
            }

            // TODO mixins


            if (updated) {
                oldRecordType = typeManager.updateRecordType(oldRecordType);
                importListener.updated(EntityType.RECORD_TYPE, null, oldRecordType.getId(), oldRecordType.getVersion());
            } else {
                importListener.existsAndEqual(EntityType.RECORD_TYPE, null, oldRecordType.getId());
            }
            return oldRecordType;
        } else {
            RecordType createdRecordType = typeManager.createRecordType(newRecordType);
            importListener.created(EntityType.RECORD_TYPE, null, createdRecordType.getId());
            return createdRecordType;
        }
    }

    public Record importRecord(Record newRecord) throws RepositoryException {
        Record oldRecord = null;
        if (newRecord.getId() != null) {
            try {
                oldRecord = repository.read(newRecord.getId());
            } catch (RecordNotFoundException e) {
                // ok
            }
        }

        if (oldRecord != null) {
            if (newRecord.softEquals(oldRecord)) {
                importListener.existsAndEqual(EntityType.RECORD, null, newRecord.getId().toString());
                return oldRecord;
            } else {
                // Delete fields which are not present in the new record anymore
                for (Map.Entry<QName, Object> field : oldRecord.getFields().entrySet()) {
                    if (!newRecord.hasField(field.getKey())) {
                        newRecord.delete(field.getKey(), true);
                    }
                }
                Record updatedRecord = repository.update(newRecord);
                importListener.updated(EntityType.RECORD, null, updatedRecord.getId().toString(), updatedRecord.getVersion());
                return updatedRecord;
            }
        } else {
            Record createdRecord = repository.create(newRecord);
            importListener.created(EntityType.RECORD, null, createdRecord.getId().toString());
            return createdRecord;
        }
    }

    private void checkEquals(Object oldValue, Object newValue, EntityType entityType, String propName, String entityName)
            throws ImportConflictException {
        if (!oldValue.equals(newValue)) {
            importListener.conflict(entityType, entityName, propName, oldValue, newValue);
        }
    }
}
