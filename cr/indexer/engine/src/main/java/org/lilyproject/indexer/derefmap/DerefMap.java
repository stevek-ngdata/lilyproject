package org.lilyproject.indexer.derefmap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;

/**
 * A DerefMap allows to keep track of dependencies between indexed records (due to dereferencing) in the scope of a
 * given (solr) index.
 */
public interface DerefMap {

    /**
     * Update the set of dependencies on specific fields of specific records for a given dependant record in a given
     * vtag. The set of fields for a certain dependency is allowed to be empty, in which case the dependency is simply
     * towards the whole record rather than to a specific field.
     *
     * @param dependantRecordId record id of the record to update dependencies for
     * @param dependantVtagId   vtag of the record to update dependencies for
     * @param newDependencies   record ids and vtags on which the given record depends, mapped on all the fields of
     *                          this record on which the dependant depends
     */
    void updateDependencies(final RecordId dependantRecordId, final SchemaId dependantVtagId,
                            Map<DependencyEntry, Set<SchemaId>> newDependencies) throws IOException;

    /**
     * Find all record ids which depend on one of the given fields of a given record in a given vtag. Both the set of
     * fields and the vtag can be <code>null</code> if you want to ignore filtering on the fields and/or vtag.
     *
     * @param dependency the record to find dependant record ids for
     * @param fields     the field (on of the fields in the set) of the given dependency which is dereferenced in the
     *                   dependant, <code>null</code> to ignore
     * @param vtag       vtag of the dependant you are interested in, <code>null</code> to ignore
     * @return iterator
     */
    DependantRecordIdsIterator findDependantsOf(final RecordId dependency, Set<SchemaId> fields, SchemaId vtag)
            throws IOException;

    /**
     * Same as {@link #findDependantsOf(org.lilyproject.repository.api.RecordId, java.util.Set,
     * org.lilyproject.repository.api.SchemaId)} but with a single field in stead of a set of fields.
     */
    DependantRecordIdsIterator findDependantsOf(final RecordId dependency, SchemaId field, SchemaId vtag)
            throws IOException;

    /**
     * Same as {@link #findDependantsOf(org.lilyproject.repository.api.RecordId, java.util.Set,
     * org.lilyproject.repository.api.SchemaId)} but with <code>null</code> as fields and vtag.
     */
    DependantRecordIdsIterator findDependantsOf(final RecordId dependency)
            throws IOException;

}
