package org.lilyproject.util.repo;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

import java.util.Map;

public class RecordTypeUtil {
    /**
     * Checks if the given record type is a subtype of another record type, or is equal to it.
     *
     * <p>This implementation ignores the version aspect of record types: it always navigates to the latest version
     * of the supertypes, which might not, but usually will, correspond to reality.</p>
     */
    public static boolean isSubtypeOrEqual(QName subRecordType, QName superRecordType, TypeManager typeManager)
            throws InterruptedException, RepositoryException {
        if (subRecordType.equals(superRecordType)) {
            return true;
        }

        // Check the ancestor record types
        SchemaId superRecordTypeId = typeManager.getRecordTypeByName(superRecordType, null).getId();
        RecordType subRecordTypeObject = typeManager.getRecordTypeByName(subRecordType, null);
        return searchParentsLatestVersion(subRecordTypeObject, superRecordTypeId, typeManager);
    }

    private static boolean searchParentsLatestVersion(RecordType recordType, SchemaId searchedRecordType,
            TypeManager typeManager) throws InterruptedException, RepositoryException {

        for (Map.Entry<SchemaId, Long> supertypeEntry : recordType.getSupertypes().entrySet()) {
            if (supertypeEntry.getKey().equals(searchedRecordType)) {
                return true;
            }

            if (searchParentsLatestVersion(typeManager.getRecordTypeById(supertypeEntry.getKey(), null),
                    searchedRecordType, typeManager)) {
                return true;
            }
        }

        return false;
    }
}
