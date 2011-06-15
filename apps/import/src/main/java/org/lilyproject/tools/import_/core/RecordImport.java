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
package org.lilyproject.tools.import_.core;

import org.lilyproject.repository.api.*;

import java.util.List;

//
// This class stems from a time when the repository did not yet offer 'create or update' behavior, nor did it
// have the Record.ResponseStatus to indicate what happened. At that time, that kind of functionality was
// implemented here. Now that the repository actually has this, there is probably little reason to keep this
// class, but for now we keep it since RecordTypeImport etc. don't have native repo support yet.
//

public class RecordImport {
    public static ImportResult<Record> importRecord(Record newRecord, ImportMode impMode, Repository repository)
            throws RepositoryException, InterruptedException {
        return importRecord(newRecord, impMode, null, repository);
    }

    public static ImportResult<Record> importRecord(Record newRecord, ImportMode impMode,
            List<MutationCondition> conditions, Repository repository)
            throws RepositoryException, InterruptedException {

        // If the user specified both record type name and version, we assume he wants to use that version, otherwise
        // move to the latest version
        boolean useLatestRecordType = newRecord.getRecordTypeName() == null ||
                newRecord.getRecordTypeVersion() == null;

        ImportResult<Record> result;

        switch (impMode) {
            case UPDATE:
                if (newRecord.getId() == null) {
                    return ImportResult.cannotUpdateDoesNotExist();
                }
                try {
                    Record updatedRecord = repository.update(newRecord, false, useLatestRecordType, conditions);

                    switch (updatedRecord.getResponseStatus()) {
                        case UP_TO_DATE:
                            result = ImportResult.upToDate(updatedRecord);
                            break;
                        case UPDATED:
                            result = ImportResult.updated(updatedRecord);
                            break;
                        case CONFLICT:
                            result = ImportResult.conditionConflict(updatedRecord);
                            break;
                        default:
                            throw new RuntimeException("Unexpected status: " + updatedRecord.getResponseStatus());
                    }
                } catch (RecordNotFoundException e) {
                    result = ImportResult.cannotUpdateDoesNotExist();
                }
                break;
            case CREATE_OR_UPDATE:
                if (newRecord.getId() == null) {
                    return ImportResult.cannotUpdateDoesNotExist();
                }

                Record updatedRecord = repository.createOrUpdate(newRecord, useLatestRecordType);

                switch (updatedRecord.getResponseStatus()) {
                    case UP_TO_DATE:
                        result = ImportResult.upToDate(updatedRecord);
                        break;
                    case UPDATED:
                        result = ImportResult.updated(updatedRecord);
                        break;
                    case CREATED:
                        result = ImportResult.created(updatedRecord);
                        break;
                    default:
                        throw new RuntimeException("Unexpected status: " + updatedRecord.getResponseStatus());
                }
                break;
            case CREATE:
                try {
                    Record createdRecord = repository.create(newRecord);
                    result = ImportResult.created(createdRecord);
                } catch (RecordExistsException e) {
                    result = ImportResult.cannotCreateExists();
                }
                break;
            default:
                throw new RuntimeException("Unexpected import mode: " + impMode);
        }

        return result;
    }
}
