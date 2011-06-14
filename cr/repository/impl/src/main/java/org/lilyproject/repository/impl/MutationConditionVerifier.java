package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.SystemFields;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MutationConditionVerifier {
    /**
     * Checks a set of conditions, if not satisfied it returns false and sets the response
     * status in the supplied record object to conflict, as well as reduces the fields to those
     * that were submitted.
     *
     * @param record the complete record state as currently stored in the repository
     * @param conditions the conditions that should be satisfied
     */
    public static boolean checkConditions(Record record, List<MutationCondition> conditions, Repository repository,
            Record newRecord) throws RepositoryException, InterruptedException {

        if (conditions == null) {
            return true;
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
            // reduce the fields to return to those that were submitted
            reduceFields(record, newRecord.getFields().keySet());
            record.setResponseStatus(ResponseStatus.CONFLICT);
            return false;
        }

        return true;
    }

    private static boolean checkValue(MutationCondition cond, Object currentValue, TypeManager typeManager)
            throws RepositoryException, InterruptedException {

        if (cond.getOp() == CompareOp.EQUAL) {
            return currentValue.equals(cond.getValue());
        } else if (cond.getOp() == CompareOp.NOT_EQUAL) {
            return !currentValue.equals(cond.getValue());
        }

        ValueType valueType = typeManager.getFieldTypeByName(cond.getField()).getValueType();
        Comparator comparator = valueType.getPrimitive().getComparator();

        if (!valueType.isPrimitive()) {
            throw new RepositoryException("Other than (not-)equal operator in mutation condition is only allowed for "
                    + "single-valued fields. Condition on field: " + cond.getField());
        }

        if (comparator == null) {
            throw new RepositoryException("Other than (not-)equals operator in mutation condition used for value type "
                    + "that does not support comparison: " + valueType.getPrimitive().getName());
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
