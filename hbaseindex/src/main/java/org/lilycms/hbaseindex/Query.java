package org.lilycms.hbaseindex;

import java.util.ArrayList;
import java.util.List;

/**
 * Description of query.
 *
 * <p>A query is performed by instantiating this object, adding conditions
 * to it, and then passing it to {@link Index#performQuery}.
 *
 * <p>A query can contain equals conditions on zero or more fields,
 * and at most one range condition. The range condition should always
 * be on the last used field. A query does not need to use all fields
 * defined in the index, but you have to use them 'left to right'.
 *
 * <p>The structural validity of the query will be checked once the
 * query is supplied to {@link Index#performQuery}, not while adding
 * the individual conditions. 
 */
public class Query {
    private List<EqualsCondition> eqConditions = new ArrayList<EqualsCondition>();
    private RangeCondition rangeCondition;

    /**
     * Adds an equals condition.
     *
     * <p>The order in which the conditions are added to the query
     * does not matter.
     *
     * @param name matching the name of the field in the {@link IndexDefinition}
     * @param value value of the correct type, or null
     */
    public void addEqualsCondition(String name, Object value) {
        eqConditions.add(new EqualsCondition(name, value));
    }

    public void setRangeCondition(String name, Object fromValue, Object toValue) {
        rangeCondition = new RangeCondition(name, fromValue, toValue);
    }

    public List<EqualsCondition> getEqConditions() {
        return eqConditions;
    }

    public EqualsCondition getCondition(String field) {
        for (EqualsCondition cond : eqConditions) {
            if (cond.name.equals(field)) {
                return cond;
            }
        }
        return null;
    }

    public RangeCondition getRangeCondition() {
        return rangeCondition;
    }

    public static class EqualsCondition {
        private String name;
        private Object value;

        public EqualsCondition(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class RangeCondition {
        private String name;
        private Object fromValue;
        private Object toValue;

        public RangeCondition(String name, Object fromValue, Object toValue) {
            this.name = name;
            this.fromValue = fromValue;
            this.toValue = toValue;
        }

        public String getName() {
            return name;
        }

        public Object getFromValue() {
            return fromValue;
        }

        public Object getToValue() {
            return toValue;
        }
    }
}
