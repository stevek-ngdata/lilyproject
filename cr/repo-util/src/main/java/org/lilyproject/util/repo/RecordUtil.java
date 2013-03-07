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

import java.util.Map;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class RecordUtil {
    private RecordUtil() {
    }

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
