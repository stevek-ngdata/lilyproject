package org.lilyproject.indexer.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ArgumentValidator;

/**
 * A DerefMap allows to keep track of dependencies between indexed records (due to dereferencing) in the scope of a
 * given (solr) index.
 *
 * @author Jan Van Besien
 */
public interface DerefMap {

    /**
     * Description of a record on which is being depended. It contains the record id and the vtag of the record on
     * which is depended (i.e. the dependency).
     */
    final static class Dependency {

        /**
         * The record id on which we have a dependency.
         */
        private final RecordId recordId;

        /**
         * The vtag of the record on which we have a dependency.
         */
        private final SchemaId vtag;

        public Dependency(RecordId recordId, SchemaId vtag) {
            ArgumentValidator.notNull(recordId, "recordId");
            ArgumentValidator.notNull(vtag, "vtag");

            this.recordId = recordId;
            this.vtag = vtag;
        }

        public RecordId getRecordId() {
            return recordId;
        }

        public SchemaId getVtag() {
            return vtag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Dependency that = (Dependency) o;

            if (recordId != null ? !recordId.equals(that.recordId) : that.recordId != null) return false;
            if (vtag != null ? !vtag.equals(that.vtag) : that.vtag != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = recordId != null ? recordId.hashCode() : 0;
            result = 31 * result + (vtag != null ? vtag.hashCode() : 0);
            return result;
        }
    }

    /**
     * An entry in the dereference map, used when updating information in the dereference map.
     */
    final static class DependencyEntry {

        /**
         * Identification of the record on which we depend. Note that if we want to express a dependency on all records
         * with a certain variant property set (with any value), this/these particular variant property/properties
         * have to be stripped of the record id, and have to be added to the set of more dimensioned variants.
         */
        private final Dependency dependency;

        /**
         * A set of variant properties that specify the variant properties that have to be added to the record id (with
         * any value) in order to come to the actual record ids on which we have a dependency.
         */
        private final Set<String> moreDimensionedVariants;

        public DependencyEntry(Dependency dependency) {
            this(dependency, Collections.<String>emptySet());
        }

        public DependencyEntry(Dependency dependency, Set<String> moreDimensionedVariants) {
            ArgumentValidator.notNull(dependency, "dependency");
            ArgumentValidator.notNull(moreDimensionedVariants, "moreDimensionedVariants");

            this.dependency = dependency;
            this.moreDimensionedVariants = Collections.unmodifiableSet(moreDimensionedVariants);
        }

        public Dependency getDependency() {
            return dependency;
        }

        public Set<String> getMoreDimensionedVariants() {
            return moreDimensionedVariants;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DependencyEntry entry = (DependencyEntry) o;

            if (dependency != null ? !dependency.equals(entry.dependency) : entry.dependency != null)
                return false;
            if (moreDimensionedVariants != null ? !moreDimensionedVariants.equals(entry.moreDimensionedVariants) :
                    entry.moreDimensionedVariants != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = dependency != null ? dependency.hashCode() : 0;
            result = 31 * result + (moreDimensionedVariants != null ? moreDimensionedVariants.hashCode() : 0);
            return result;
        }
    }

    static interface DependantRecordIdsIterator extends Closeable {
        boolean hasNext() throws IOException;

        RecordId next() throws IOException;
    }

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
     * Find all record ids which depend on a given field of a given record in a given vtag. The given field can be
     * <code>null</code>, which translates to "find all record ids which depend on a given record". That is because
     * sometimes a dereference expression leads to a dependency on a particular field, sometimes directly on the whole
     * record (e.g. +foo).
     *
     * @param dependency the record to find dependant record ids for
     * @param field      the field of the given dependency which is dereferenced in the dependant, >code>null</code> to
     *                   ignore
     * @return iterator
     */
    DependantRecordIdsIterator findDependantsOf(final Dependency dependency, SchemaId field)
            throws IOException;
}
