package org.lilyproject.indexer.integration;

import org.lilyproject.indexer.model.util.IndexInfo;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.plugin.PluginRegistry;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.spi.RecordUpdateHook;
import org.lilyproject.util.repo.RecordEvent;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Set;

/**
 * A record update hook that adds information to the RecordEvent needed to select what indexes
 * need to be informed about this record update.
 */
public class IndexSelectionRecordUpdateHook implements RecordUpdateHook {
    private PluginRegistry pluginRegistry;
    private IndexesInfo indexesInfo;

    /**
     * Name should be unique among all RecordUpdateHook's. Because the name starts
     * with 'org.lilyproject', this hook does not need to be activated through
     * configuration.
     */
    private String NAME = "org.lilyproject.IndexSelectionRecordUpdateHook";

    public IndexSelectionRecordUpdateHook(IndexesInfo indexesInfo) {
        this.indexesInfo = indexesInfo;
    }

    public IndexSelectionRecordUpdateHook(PluginRegistry pluginRegistry, IndexesInfo indexesInfo) {
        this.pluginRegistry = pluginRegistry;
        this.indexesInfo = indexesInfo;

        pluginRegistry.addPlugin(RecordUpdateHook.class, NAME, this);
    }

    @PreDestroy
    public void destroy() {
        // Use same arguments as for addPlugin
        pluginRegistry.removePlugin(RecordUpdateHook.class, NAME, this);
    }

    @Override
    public void beforeUpdate(Record record, Record originalRecord, Repository repository, FieldTypes fieldTypes,
            RecordEvent recordEvent) throws RepositoryException, InterruptedException {
        Collection<IndexInfo> indexInfos = indexesInfo.getIndexInfos();
        if (indexInfos.size() > 0) {
            TypeManager typeMgr = repository.getTypeManager();

            RecordEvent.IndexSelection idxSel = new RecordEvent.IndexSelection();
            recordEvent.setIndexSelection(idxSel);

            if (indexesInfo.getRecordFilterDependsOnRecordType()) {
                // Because record type names can change, this is not guaranteed to be the same as what gets
                // stored in the repo, but that doesn't matter as it is only for indexing purposes.
                idxSel.setNewRecordType(typeMgr.getRecordTypeByName(record.getRecordTypeName(), null).getId());
                idxSel.setOldRecordType(typeMgr.getRecordTypeByName(originalRecord.getRecordTypeName(), null).getId());
            }

            Set<QName> names = indexesInfo.getRecordFilterFieldDependencies();
            for (QName name : names) {
                Object oldValue = null, newValue = null;
                if (record.hasField(name)) {
                    newValue = record.getField(name);
                }
                if (originalRecord.hasField(name)) {
                    oldValue = originalRecord.getField(name);
                }
                if (oldValue != null || newValue != null) {
                    FieldType type = fieldTypes.getFieldType(name);
                    addField(type, oldValue, newValue, idxSel);
                }
            }
        }
    }

    @Override
    public void beforeCreate(Record newRecord, Repository repository, FieldTypes fieldTypes, RecordEvent recordEvent)
            throws RepositoryException, InterruptedException {

        Collection<IndexInfo> indexInfos = indexesInfo.getIndexInfos();
        if (indexInfos.size() > 0) {
            TypeManager typeMgr = repository.getTypeManager();

            RecordEvent.IndexSelection idxSel = new RecordEvent.IndexSelection();
            recordEvent.setIndexSelection(idxSel);

            if (indexesInfo.getRecordFilterDependsOnRecordType()) {
                idxSel.setNewRecordType(typeMgr.getRecordTypeByName(newRecord.getRecordTypeName(), null).getId());
            }

            Set<QName> names = indexesInfo.getRecordFilterFieldDependencies();
            for (QName name : names) {
                if (newRecord.hasField(name)) {
                    Object newValue = newRecord.getField(name);
                    FieldType type = fieldTypes.getFieldType(name);
                    addField(type, null, newValue, idxSel);
                }
            }
        }
    }

    @Override
    public void beforeDelete(Record originalRecord, Repository repository, FieldTypes fieldTypes,
            RecordEvent recordEvent) throws RepositoryException, InterruptedException {
        Collection<IndexInfo> indexInfos = indexesInfo.getIndexInfos();
        if (indexInfos.size() > 0) {
            TypeManager typeMgr = repository.getTypeManager();

            RecordEvent.IndexSelection idxSel = new RecordEvent.IndexSelection();
            recordEvent.setIndexSelection(idxSel);

            if (indexesInfo.getRecordFilterDependsOnRecordType()) {
                idxSel.setOldRecordType(typeMgr.getRecordTypeByName(originalRecord.getRecordTypeName(), null).getId());
            }

            Set<QName> names = indexesInfo.getRecordFilterFieldDependencies();
            for (QName name : names) {
                if (originalRecord.hasField(name)) {
                    Object oldValue = originalRecord.getField(name);
                    FieldType type = fieldTypes.getFieldType(name);
                    addField(type, oldValue, null, idxSel);
                }
            }
        }
    }

    private void addField(FieldType type, Object oldValue, Object newValue, RecordEvent.IndexSelection idxSel)
            throws RepositoryException, InterruptedException {

        if (oldValue == null && newValue == null) {
            return;
        }

        if (oldValue != null) {
            oldValue = type.getValueType().toBytes(oldValue, new IdentityRecordStack());
        }

        if (newValue != null) {
            newValue = type.getValueType().toBytes(newValue, new IdentityRecordStack());
        }

        idxSel.addChangedField(type.getId(), (byte[])oldValue, (byte[])newValue);
    }
}
