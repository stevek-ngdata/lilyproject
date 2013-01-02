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
package org.lilyproject.repository.impl;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ResponseStatus;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.repo.SystemFields;

public class MutationConditionVerifier {
    /**
     * Checks a set of conditions, if not satisfied it returns false and sets the response
     * status in the supplied record object to conflict, as well as reduces the fields to those
     * that were submitted.
     *
     * @param record     the complete record state as currently stored in the repository
     * @param conditions the conditions that should be satisfied
     * @return null if conditions are satisfied, a Record object if not.
     */
    public static Record checkConditions(Record record, List<MutationCondition> conditions, Repository repository,
                                         Record newRecord) throws RepositoryException, InterruptedException {

        if (conditions == null) {
            return null;
        }

        boolean allSatisfied = true;

        // TODO should we check with the typemanager that the supplied value types are
        // of the correct type? this is not necessary for us, but might be helpful to the
        // user. OTOH, since users most of the time will go through a remote itf, such checks
        // will likely already have happened as part of serialization/deserialization code.

        SystemFields systemFields = SystemFields.getInstance(repository.getTypeManager(), repository.getIdGenerator());

        for (MutationCondition condition : conditions) {
            Object value = systemFields.softEval(record, condition.getField(), repository.getTypeManager());

            // Compare with null value is special case, handle this first
            if (condition.getValue() == null) {
                if (condition.getOp() == CompareOp.EQUAL) {
                    if (value == null) {
                        continue;
                    } else {
                        allSatisfied = false;
                        break;
                    }
                } else if (condition.getOp() == CompareOp.NOT_EQUAL) {
                    if (value != null) {
                        continue;
                    } else {
                        allSatisfied = false;
                        break;
                    }
                } else {
                    throw new RepositoryException("When comparing to null, only (not-)equal operator is allowed. " +
                            "Operator: " + condition.getOp() + ", field: " + condition.getField());
                }
            }

            if (value == null && condition.getAllowMissing()) {
                continue;
            }

            if (value == null) {
                allSatisfied = false;
                break;
            }

            if (!checkValue(condition, value, repository.getTypeManager())) {
                allSatisfied = false;
                break;
            }
        }

        if (!allSatisfied) {
            Record responseRecord = record.cloneRecord();
            if (newRecord != null) {
                // reduce the fields to return to those that were submitted
                reduceFields(responseRecord, newRecord.getFields().keySet());
            } else {
                // reduce the fields to those on which conditions were put
                reduceFields(responseRecord, extractFields(conditions));
            }
            responseRecord.setResponseStatus(ResponseStatus.CONFLICT);
            return responseRecord;
        }

        return null;
    }

    private static Set<QName> extractFields(List<MutationCondition> conditions) {
        Set<QName> fields = new HashSet<QName>();
        for (MutationCondition condition : conditions) {
            fields.add(condition.getField());
        }
        return fields;
    }

    private static boolean checkValue(MutationCondition cond, Object currentValue, TypeManager typeManager)
            throws RepositoryException, InterruptedException {

        if (cond.getOp() == CompareOp.EQUAL) {
            return currentValue.equals(cond.getValue());
        } else if (cond.getOp() == CompareOp.NOT_EQUAL) {
            return !currentValue.equals(cond.getValue());
        }

        ValueType valueType = typeManager.getFieldTypeByName(cond.getField()).getValueType();
        Comparator comparator = valueType.getComparator();

        if (comparator == null) {
            throw new RepositoryException("Other than (not-)equals operator in mutation condition used for value type "
                    + "that does not support comparison: " + valueType.getName());
        }

        int result = comparator.compare(currentValue, cond.getValue());

        if (result == 0) {
            switch (cond.getOp()) {
                case EQUAL:
                case GREATER_OR_EQUAL:
                case LESS_OR_EQUAL:
                    return true;
                default:
                    return false;
            }
        } else if (result < 0) {
            switch (cond.getOp()) {
                case LESS:
                case LESS_OR_EQUAL:
                case NOT_EQUAL:
                    return true;
                default:
                    return false;
            }
        } else { // result > 0
            switch (cond.getOp()) {
                case GREATER:
                case GREATER_OR_EQUAL:
                case NOT_EQUAL:
                    return true;
                default:
                    return false;
            }
        }
    }

    private static void reduceFields(Record record, Set<QName> fieldsToInclude) {
        Iterator<QName> it = record.getFields().keySet().iterator();

        while (it.hasNext()) {
            QName name = it.next();
            if (!fieldsToInclude.contains(name)) {
                it.remove();
            }
        }
    }
}
