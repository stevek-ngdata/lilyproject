package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

public class HBaseRecordFilterList implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordFilterList)) {
            return null;
        }

        RecordFilterList filter = (RecordFilterList)uncastFilter;

        FilterList.Operator hbaseOp = filter.getOperator() == RecordFilterList.Operator.MUST_PASS_ONE ?
                FilterList.Operator.MUST_PASS_ONE : FilterList.Operator.MUST_PASS_ALL;

        FilterList hbaseFilter = new FilterList(hbaseOp);

        for (RecordFilter subFilter : filter.getFilters()) {
            hbaseFilter.addFilter(factory.createHBaseFilter(subFilter, repository, factory));
        }

        return hbaseFilter;
    }
}
