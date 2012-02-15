package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.lilyproject.repository.api.CompareOp;

public class HBaseRecordFilterUtil {
    public static CompareFilter.CompareOp translateCompareOp(CompareOp compareOp) {
        CompareFilter.CompareOp result;
        switch (compareOp) {
            case EQUAL:
                result = CompareFilter.CompareOp.EQUAL;
                break;
            case NOT_EQUAL:
                result = CompareFilter.CompareOp.NOT_EQUAL;
                break;
            case GREATER:
                result = CompareFilter.CompareOp.GREATER;
                break;
            case GREATER_OR_EQUAL:
                result = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
            case LESS:
                result = CompareFilter.CompareOp.LESS;
                break;
            case LESS_OR_EQUAL:
                result = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
            default:
                throw new RuntimeException("Unrecognized CompareOp: " + compareOp);
        }
        return result;
    }
}
