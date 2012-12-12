package org.lilyproject.indexer.model.util;

import org.lilyproject.util.Pair;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.repo.RecordEvent;

public class IndexRecordFilterUtil {
    /**
     * Returns Record instances that can be used to evaluate an IndexerConf's IndexRecordFilter. The record
     * instances are created based on state stored in the RecordEvent by the IndexRecordFilterHook.
     *
     * <p>The IndexRecordFilterHook puts the fields & record type info needed by each of the
     * indexes known at that time in the RecordEvent.</p>
     *
     * <p>Of course, it can happen that indexerconfs have been changed, or new indexes have been added,
     * since the RecordEvent was created. It can also be that the 'new' record state stored in
     * the RecordEvent doesn't correspond to the record state you would get now when reading the
     * record. It is up to the caller to decide how to deal with these situations.</p>
     *
     * <p>This method returns always two Record instances: the old and new record state, even if
     * there would be no old instance (in case of record creation) or new (in case of record deletion).
     * In those cases, the Record objects will be empty, hence will be more generic, and hence
     * will only match IndexRecordFilter's that match all records anyway, so they won't match
     * more filters than either the old or new record state would match.</p>
     */
    public static Pair<Record,Record> getOldAndNewRecordForRecordFilterEvaluation(RecordId recordId, RecordEvent recordEvent,
            Repository repository) throws RepositoryException, InterruptedException {

        //
        // Create the 'old' and 'new' Record instances.
        //
        // The RecordEvent contains the fields & record type info needed by the filters of the different
        // indexes, this is taken care of by IndexRecordFilterHook.
        //
        // Of course, it can happen that indexerconfs have been changed, or new indexes have been added,
        // since the event was created, and then this information will be missing. We could check for that
        // and in case of doubt send the event to the index anyway. This approach however also has the
        // disadvantage that, in case there are a lot of outstanding events in the queue, that they
        // might be sent to indexes that only expect a low update rate. Besides, it also complicates the
        // code. So we go for the simple approach: when the indexerconfs change, there is a transition
        // period to be expected, and one might need to rebuild indexes.
        //

        RecordEvent.IndexRecordFilterData idxSel = recordEvent.getIndexRecordFilterData();
        if (idxSel != null) {
            Record newRecord = idxSel.getNewRecordExists() ? repository.newRecord(recordId) : null;
            Record oldRecord = idxSel.getOldRecordExists() ? repository.newRecord(recordId) : null;

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

            return Pair.create(oldRecord, newRecord);
        }

        return Pair.create(null, null);
    }
}
