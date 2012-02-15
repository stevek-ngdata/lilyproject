package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.apache.hadoop.hbase.filter.Filter;

public interface HBaseRecordFilterFactory {
    /**
     * The factories are called one by one: if the implementation doesn't support the supplied RecordFilter,
     * it should return null.
     *
     * @param factory to be used for creating nested filters
     */
    Filter createHBaseFilter(RecordFilter filter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException;
}
