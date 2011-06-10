package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.ResponseStatus;

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
    public static boolean checkConditions(Record record, List<MutationCondition> conditions, Record newRecord) {
        if (conditions == null) {
            return true;
        }

        boolean allSatisfied = true;

        // TODO should we check with the typemanager that the supplied value types are
        // of the correct type? this is not necessary for us, but might be helpful to the
        // user. OTOH, since users most of the time will go through a remote itf, such checks
        // will likely already have happened as part of serialization/deserialization code.

        for (MutationCondition condition : conditions) {
            if (!record.hasField(condition.getField())) {
                allSatisfied = false;
                break;
            }
            Object fieldValue = record.getField(condition.getField());
            if (!fieldValue.equals(condition.getValue())) {
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
