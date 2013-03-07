package org.lilyproject.indexer.derefmap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.SchemaId;

/**
 * A DerefMap allows to keep track of dependencies between indexed records (due to dereferencing) in the scope of a
 * given (solr) index.
 * <p>
 * The terminology used is as follows:
 * <ul>
 * <li>parent record: a record that has other records depending on it</li>
 * <li>dependency: a record that depends on a parent record
 * </ul>
 *
 * There is typically a one-to-many relationship between parent records and dependencies
 */
public interface DerefMap {

    /**
     * Update the set of dependencies on specific fields of specific records for a given dependant record in a given
     * vtag. The set of fields for a certain dependency is allowed to be empty, in which case the dependency is simply
     * towards the whole record rather than to a specific field.
     *
     * @param parentRecordId record id of the record to update dependencies for
     * @param dependantVtagId   vtag of the record to update dependencies for
     * @param newDependantEntries   record ids and vtags on which the given record depends, mapped on all the fields of
     *                          this record on which the dependant depends
     */
    void updateDependants(AbsoluteRecordId parentRecordId, final SchemaId dependantVtagId,
                            Map<DependencyEntry, Set<SchemaId>> newDependantEntries) throws IOException;

    /**
     * Find all record ids which depend on one of the given fields of a given record in a given vtag. Both the set of
     * fields and the vtag can be <code>null</code> if you want to ignore filtering on the fields and/or vtag.
     *
     * @param parentRecordId the record to find dependant record ids for
     * @param fields     the field (on of the fields in the set) of the given dependency which is dereferenced in the
     *                   dependant, <code>null</code> to ignore
     * @param vtag       vtag of the dependant you are interested in, <code>null</code> to ignore
     * @return iterator
     */
    DependantRecordIdsIterator findDependantsOf(AbsoluteRecordId parentRecordId,
            Set<SchemaId> fields, SchemaId vtag) throws IOException;

    /**
     * Same as {@link #findDependantsOf(org.lilyproject.repository.api.RecordId, java.util.Set,
     * org.lilyproject.repository.api.SchemaId)} but with a single field in stead of a set of fields.
     */
    DependantRecordIdsIterator findDependantsOf(AbsoluteRecordId parentRecordId, SchemaId field, SchemaId vtag)
            throws IOException;

    /**
     * Same as {@link #findDependantsOf(org.lilyproject.repository.api.RecordId, java.util.Set,
     * org.lilyproject.repository.api.SchemaId)} but with <code>null</code> as fields and vtag.
     */
    DependantRecordIdsIterator findDependantsOf(AbsoluteRecordId parentRecordId)
            throws IOException;

}
