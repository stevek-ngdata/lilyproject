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
package org.lilyproject.repository.api;

import org.lilyproject.util.ArgumentValidator;

/**
 * A condition specified when doing a conditional mutation (update, delete) on the {@link Repository}.
 * <p/>
 * <p>For full details see the constructor {@link #MutationCondition(QName, CompareOp, Object, boolean)}.</p>
 */
public class MutationCondition {
    private QName field;
    private CompareOp op;
    private Object value;
    private boolean allowMissing;

    /**
     * Creates a mutation condition with the operator equals and allowMissing=false.
     * <p/>
     * See {@link #MutationCondition(QName, CompareOp, Object, boolean)}.
     */
    public MutationCondition(QName field, Object value) {
        this(field, CompareOp.EQUAL, value, false);
    }

    /**
     * Creates a mutation condition with the operator equals.
     * <p/>
     * See {@link #MutationCondition(QName, CompareOp, Object, boolean)}.
     */
    public MutationCondition(QName field, Object value, boolean allowMissing) {
        this(field, CompareOp.EQUAL, value, allowMissing);
    }

    /**
     * Creates a mutation condition with allowMissing=false.
     * <p/>
     * See {@link #MutationCondition(QName, CompareOp, Object, boolean)}.
     */
    public MutationCondition(QName field, CompareOp op, Object value) {
        this(field, op, value, false);
    }

    /**
     * Creates a mutation condition.
     *
     * @param field        name of the field to check. To check on the version of the record, supply
     *                     new QName("org.lilyproject.system", "version"), the supplied value should be a Long.
     * @param op           the operator to compare the record value against the value in this condition (the record
     *                     value is on the left hand side of the comparison). Some value types only support (not-)equals
     *                     conditions.
     * @param value        A value corresponding to the type of the specified field
     *                     (see {@link TypeManager#getValueType(String, boolean, boolean)}). Allowed to be null for
     *                     simple field presence check: when used with equal operator, tests field
     *                     is missing, when used with not equal operator, tests field is present with any value.
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
