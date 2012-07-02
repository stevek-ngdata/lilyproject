package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Multimap;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;

/**
 * A DerefMap allows to keep track of dependencies between indexed records (due to dereferencing) in the scope of a
 * given index.
 *
 * @author Jan Van Besien
 */
public interface DerefMap {

    // TODO: and how to get an instance of a deref map?

    /**
     * An entry in the dereference map is a tuple of record id and version tag.
     */
    final static class Entry {
        private final RecordId recordId;
        private final SchemaId vtag;

        public Entry(RecordId recordId, SchemaId vtag) {
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

            Entry entry = (Entry) o;

            if (recordId != null ? !recordId.equals(entry.recordId) : entry.recordId != null) return false;
            if (vtag != null ? !vtag.equals(entry.vtag) : entry.vtag != null) return false;

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
     * Find the set of record ids (and corresponding version tags) on which a given record (in a given version tag)
     * depends.
     *
     * @param recordId record id of the record to find dependencies for
     * @param vtag     vtag of the record to find dependencies for
     * @return the record ids and vtags on which the given record depends
     */
    Set<Entry> findDependencies(final RecordId recordId, final SchemaId vtag);

    /**
     * Update the set of dependencies for a given record in a given vtag. Conceptually the existing set of
     * dependencies is replaced with the new provided set. In reality, the implementation might make incremental
     * updates.
     *
     * @param dependantRecordId record id of the record to update dependencies for
     * @param dependantVtagId   vtag of the record to update dependencies for
     * @param newDependencies   record ids and vtags on which the given record depends, mapped on all the fields of
     *                          this
     *                          record on which the dependant depends
     */
    void updateDependencies(final RecordId dependantRecordId, final SchemaId dependantVtagId,
                            Multimap<Entry, SchemaId> newDependencies)
            throws IOException;

    /**
     * Find all record ids which depend on a given field of a given record in a given vtag.
     *
     * TODO: what if the record entry or field itself is not found in the derefmap?
     *
     * @param recordEntry the record entry in the deref map to find dependant record ids for
     * @param field       the field of the given record via which the dependendant records need to be found
     * @return
     */
    Iterator<RecordId> findRecordIdsWhichDependOn(final Entry recordEntry, SchemaId field);
}
