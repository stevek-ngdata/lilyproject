package org.lilyproject.repository.spi;

import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Implements the translation from a {@link RecordFilter} definition to an actual HBase filter
 * object.
 * 
 * <p>Implementations are found through Java's {@link java.util.ServiceLoader}, thus by listing
 * their fully qualified class name in META-INF/services/org.lilyproject.repository.spi.HBaseRecordFilterFactory.</p>
 *
 * <p>Most of the time, implementations will work such that they filter out entire rows rather than
 * individual KeyValue's. In any case, implementations should never filter out Lily's system fields.</p>
 */
public interface HBaseRecordFilterFactory {
    /**
     * The implementation should check whether it recognizes the supplied RecordFilter implementation:
     * if not, it should return null.
     *
     * <p>The supplied Repository object is mainly intended for TypeManager consultation.</p>
     *
     * @param factory to be used for creating nested filters
     */
    Filter createHBaseFilter(RecordFilter filter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException;
}
