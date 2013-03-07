package org.lilyproject.util.repo;

import java.util.Map;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class RecordUtil {
    /**
     * Checks if the given record is an instance of the given record type. This does an "instance of" check,
     * thus also searches the supertypes for a match.
     */
    public static boolean instanceOf(Record record, QName requestedRecordTypeName, TypeManager typeManager)
            throws InterruptedException, RepositoryException {
        if (record.getRecordTypeName() == null) {
            throw new NullPointerException("record type of record is null");
        }

        if (record.getRecordTypeName().equals(requestedRecordTypeName)) {
            return true;
        }

        // Check the ancestor record types
        SchemaId searchedRecordType = typeManager.getRecordTypeByName(requestedRecordTypeName, null).getId();
        RecordType recordType = typeManager.getRecordTypeByName(record.getRecordTypeName(), record.getRecordTypeVersion());
        return searchParents(recordType, searchedRecordType, typeManager);
    }

    /**
     * Checks if the given record is an instance of the given record type. This does an "instance of" check,
     * thus also searches the supertypes for a match.
     */
    public static boolean instanceOf(Record record, SchemaId requestedRecordTypeId, TypeManager typeManager)
            throws InterruptedException, RepositoryException {
        if (record.getRecordTypeName() == null) {
            throw new NullPointerException("record type of record is null");
        }


        RecordType recordType = typeManager.getRecordTypeByName(record.getRecordTypeName(), record.getRecordTypeVersion());

        if (recordType.getId().equals(requestedRecordTypeId)) {
            return true;
        }

        // Check the ancestor record types
        return searchParents(recordType, requestedRecordTypeId, typeManager);
    }

    private static boolean searchParents(RecordType recordType, SchemaId searchedRecordType, TypeManager typeManager)
            throws InterruptedException, RepositoryException {

        for (Map.Entry<SchemaId, Long> supertypeEntry : recordType.getSupertypes().entrySet()) {
            if (supertypeEntry.getKey().equals(searchedRecordType)) {
                return true;
            }

            if (searchParents(typeManager.getRecordTypeById(supertypeEntry.getKey(), supertypeEntry.getValue()),
                    searchedRecordType, typeManager)) {
                return true;
            }
        }

        return false;
    }
}
