package org.lilyproject.repository.api.filter;

/**
 * A RecordFilter filters records when performing scan operations.
 * 
 * <p>To use filters, find an implementation of this interface and supply an instance of it to
 * {@link org.lilyproject.repository.api.RecordScan#setRecordFilter(RecordFilter)}</p>
 *
 * <p>Classes extending from this interface are the specification (definition) of a filter:
 * the type of the filter with its various parameters.</p>
 *
 * <p>RecordFilters are translated to HBase filters, so they are evaluated within the
 * HBase region servers.</p>
 *
 * <p>The translation to HBase filters is performed by classes implementing the interface
 * HBaseRecordFilterFactory which is part of lily-repository-spi.</p>
 *
 */
public interface RecordFilter {
}
