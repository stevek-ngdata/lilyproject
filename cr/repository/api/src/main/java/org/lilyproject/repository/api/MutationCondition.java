package org.lilyproject.repository.api;

import org.lilyproject.util.ArgumentValidator;

public class MutationCondition {
    private QName field;
    private CompareOp op;
    private Object value;
    private boolean allowMissing;

    public MutationCondition(QName field, Object value) {
        this(field, CompareOp.EQUAL, value, false);
    }

    public MutationCondition(QName field, Object value, boolean allowMissing) {
        this(field, CompareOp.EQUAL, value, allowMissing);
    }

    public MutationCondition(QName field, CompareOp op, Object value) {
        this(field, op, value, false);
    }

    /**
     *
     * @param field name of the field to check.
     * @param op the operator to compare the record value against the value in this condition (the record
     *           value is on the left hand side of the comparison). Some value types only support (not-)equals
     *           conditions.
     * @param value allowed to be null for simple field presence check: when used with equal operator, tests field
     *              is missing, when used with not equal operator, tests field is present with any value.
     * @param allowMissing only applies when the value param is not null. When this flag is true, the condition
     *                     will be true if the field either equals the specified value or is missing.
     */
    public MutationCondition(QName field, CompareOp op, Object value, boolean allowMissing) {
        ArgumentValidator.notNull(field, "field");
        ArgumentValidator.notNull(op, "op");

        this.field = field;
        this.op = op;
        this.value = value;
        this.allowMissing = allowMissing;
    }

    public QName getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    public CompareOp getOp() {
        return op;
    }

    public boolean getAllowMissing() {
        return allowMissing;
    }
}
