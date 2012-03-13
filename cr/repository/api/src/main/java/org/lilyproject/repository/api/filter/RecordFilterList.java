package org.lilyproject.repository.api.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * A Filter which is an ordered list of other filters, which are evaluated with the specified
 * operator: {@link Operator#MUST_PASS_ALL} (= AND) or {@link Operator#MUST_PASS_ONE} (= OR).
 *
 * <p>A RecordFilterList can by itself again contain RecordFilterList's, thus enabling
 * nested boolean expressions.</p>
 */
public class RecordFilterList implements RecordFilter {
    public static enum Operator {
        MUST_PASS_ALL,
        MUST_PASS_ONE
    }

    private Operator operator = Operator.MUST_PASS_ALL;
    private List<RecordFilter> filters = new ArrayList<RecordFilter>();
    
    public RecordFilterList() {
        
    }
    
    public RecordFilterList(Operator operator) {
        this.operator = operator;
    }
    
    public void addFilter(RecordFilter filter) {
        this.filters.add(filter);
    }

    public List<RecordFilter> getFilters() {
        return filters;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }
}
