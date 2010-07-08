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
            importListener.existsAndEqual(EntityType.FIELD_TYPE, newFieldType.getName().toString());
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
                importListener.updated(EntityType.RECORD_TYPE, oldRecordType.getId(), null);
            } else {
                importListener.existsAndEqual(EntityType.RECORD_TYPE, oldRecordType.getId());
            }
            return oldRecordType;
        } else {
            RecordType createdRecordType = typeManager.createRecordType(newRecordType);
            importListener.created(EntityType.RECORD_TYPE, createdRecordType.getId(), null);
            return createdRecordType;
        }
    }

    private void checkEquals(Object oldValue, Object newValue, EntityType entityType, String propName, String entityName)
            throws ImportConflictException {
        if (!oldValue.equals(newValue)) {
            importListener.conflict(entityType, entityName, propName, oldValue, newValue);
        }
    }
}
