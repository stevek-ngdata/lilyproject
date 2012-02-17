package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

public class HBaseRecordIdPrefixFilter implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordIdPrefixFilter)) {
            return null;
        }

        RecordIdPrefixFilter filter = (RecordIdPrefixFilter)uncastFilter;

        if (filter.getRecordId() == null) {
            throw new IllegalArgumentException("Record ID should be specified in RecordIdPrefixFilter");
        }

        RecordId recordId = filter.getRecordId();
        byte[] rowKeyPrefix = recordId.toBytes();

        PrefixFilter hbaseFilter = new PrefixFilter(rowKeyPrefix);

        return hbaseFilter;
    }
}
