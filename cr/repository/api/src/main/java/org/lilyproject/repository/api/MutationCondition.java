package org.lilyproject.repository.api;

public class MutationCondition {
    private QName field;
    // private CompareOp op; // for what kinds of fields will this be defined?
    private Object value; // or use list of values
    private boolean allowMissing;

    public MutationCondition(QName field, Object value) {
        this.field = field;
        this.value = value;
    }

    public QName getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }
}
