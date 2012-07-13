package org.lilyproject.indexer.derefmap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.hbaseindex.Index;
import org.lilyproject.hbaseindex.IndexDefinition;
import org.lilyproject.hbaseindex.IndexEntry;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.hbaseindex.Query;
import org.lilyproject.hbaseindex.QueryResult;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.io.Closer;

/**
 * @author Jan Van Besien
 */
public class DerefMapHbaseImpl implements DerefMap {

    private static final byte[] DEPENDENCIES_KEY = Bytes.toBytes("dependencies");

    private static final byte[] FIELDS_KEY = Bytes.toBytes("fields");

    private static final byte[] DUMMY_IDENTIFIER = new byte[]{0};

    private Index forwardDerefIndex;

    private Index backwardDerefIndex;

    private DerefMapSerializationUtil serializationUtil;

    /**
     * Private constructor. Clients should use static factory methods {@link #delete(String,
     * org.apache.hadoop.conf.Configuration)} and {@link #create(String, org.apache.hadoop.conf.Configuration,
     * org.lilyproject.repository.api.IdGenerator)}
     */
    private DerefMapHbaseImpl(final String indexName, final Configuration hbaseConfiguration,
                              final IdGenerator idGenerator)
            throws IndexNotFoundException, IOException, InterruptedException {

        this.serializationUtil = new DerefMapSerializationUtil(idGenerator);

        final IndexManager indexManager = new IndexManager(hbaseConfiguration);

        IndexDefinition forwardIndexDef = new IndexDefinition(forwardIndexName(indexName));
        // For the record ID we use a variable length byte array field of which the first two bytes are fixed length
        // The first byte is actually the record identifier byte.
        // The second byte really is the first byte of the record id. We put this in the fixed length part
        // (safely because a record id should at least be a single byte long) because this prevents BCD encoding
        // on the first byte, thus making it easier to configure table splitting based on the original input.
        forwardIndexDef.addVariableLengthByteField("dependant_recordid", 2);
        forwardIndexDef.addByteField("dependant_vtag", DerefMapSerializationUtil.SCHEMA_ID_BYTE_LENGTH);
        forwardDerefIndex = indexManager.getIndex(forwardIndexDef);

        IndexDefinition backwardIndexDef = new IndexDefinition(backwardIndexName(indexName));
        // Same remark as in the forward index.
        backwardIndexDef.addVariableLengthByteField("dependency_masterrecordid", 2);
        backwardIndexDef.addByteField("dependant_vtag", DerefMapSerializationUtil.SCHEMA_ID_BYTE_LENGTH);
        backwardIndexDef.addVariableLengthByteField("variant_properties_pattern");
        backwardDerefIndex = indexManager.getIndex(backwardIndexDef);
    }

    /**
     * Create a DerefMap for a given index. If this is the first time the DerefMap is constructed for this index,
     * the forward and backward index tables will be created.
     *
     * @param indexName          name of the index
     * @param hbaseConfiguration hbase configuration
     * @param idGenerator        id generator
     * @throws IndexNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    public static DerefMap create(final String indexName, final Configuration hbaseConfiguration,
                                  final IdGenerator idGenerator)
            throws IndexNotFoundException, IOException, InterruptedException {
        return new DerefMapHbaseImpl(indexName, hbaseConfiguration, idGenerator);
    }

    /**
     * Delete a DerefMap. This will delete the corresponding hbase tables.
     *
     * @param indexName          name of the index to delete
     * @param hbaseConfiguration hbase configuration
     * @throws IOException
     * @throws IndexNotFoundException if the index doesn't exist (maybe it was already deleted?)
     */
    public static void delete(final String indexName, final Configuration hbaseConfiguration)
            throws IOException, IndexNotFoundException {
        final IndexManager manager = new IndexManager(hbaseConfiguration);
        manager.deleteIndex(forwardIndexName(indexName));
        manager.deleteIndex(backwardIndexName(indexName));
    }

    private static String forwardIndexName(String indexName) {
        return "deref-forward-" + indexName;
    }

    private static String backwardIndexName(String indexName) {
        return "deref-backward-" + indexName;
    }

    @Override
    public void updateDependencies(RecordId dependantRecordId, SchemaId dependantVtagId,
                                   Map<DependencyEntry, Set<SchemaId>> newDependencyEntries)
            throws IOException {
        final Set<DependencyEntry> existingEntries = findDependencies(dependantRecordId, dependantVtagId);

        // Figure out what changed
        final Set<DependencyEntry> removedDependencies =
                figureOutRemovedDependencies(newDependencyEntries.keySet(), existingEntries);
        final Collection<DependencyEntry> addedDependencies =
                figureOutAddedDependencies(newDependencyEntries.keySet(), existingEntries);

        // IMPORTANT implementation note: the order in which changes are applied is not arbitrary. It is such that if
        // the process would fail in between, there will never be left any state in the backward index which would not
        // be found via the forward index.

        // delete removed from bwd index
        for (DependencyEntry removed : removedDependencies) {
            final IndexEntry backwardEntry =
                    createBackwardEntry(removed.getDependency(), dependantRecordId, dependantVtagId, null,
                            removed.getMoreDimensionedVariants());
            backwardDerefIndex.removeEntry(backwardEntry);
        }

        // update fwd index (added and removed at the same time, it is a single row)
        final IndexEntry fwdEntry =
                createForwardEntry(dependantRecordId, dependantVtagId, newDependencyEntries.keySet());
        forwardDerefIndex.addEntry(fwdEntry);

        // add added to bwd idx
        for (DependencyEntry added : addedDependencies) {
            final Set<SchemaId> fields = newDependencyEntries.get(added);
            final IndexEntry backwardEntry =
                    createBackwardEntry(added.getDependency(), dependantRecordId, dependantVtagId, fields,
                            added.getMoreDimensionedVariants());
            backwardDerefIndex.addEntry(backwardEntry);
        }
    }

    private Set<DependencyEntry> figureOutRemovedDependencies(Collection<DependencyEntry> newDependencies,
                                                              Set<DependencyEntry> existingDependencies) {
        final Set<DependencyEntry> removed = new HashSet<DependencyEntry>();

        // add all existing
        removed.addAll(existingDependencies);

        // remove all new
        removed.removeAll(newDependencies);

        return removed;
    }

    private Collection<DependencyEntry> figureOutAddedDependencies(Set<DependencyEntry> newDependencyEntries,
                                                                   Set<DependencyEntry> existingDependencies) {
        final Set<DependencyEntry> added = new HashSet<DependencyEntry>();

        // add all new
        added.addAll(newDependencyEntries);

        // remove all existing
        added.removeAll(existingDependencies);

        return added;
    }

    private IndexEntry createForwardEntry(RecordId dependantRecordId, SchemaId dependantVtagId,
                                          Collection<DependencyEntry> newDependencies) throws IOException {
        final IndexEntry fwdEntry = new IndexEntry(forwardDerefIndex.getDefinition());
        fwdEntry.addField("dependant_recordid", dependantRecordId.toBytes());
        fwdEntry.addField("dependant_vtag", dependantVtagId.getBytes());

        // we do not really use the identifier... all we are interested in is in the data of the entry
        fwdEntry.setIdentifier(DUMMY_IDENTIFIER);

        // the data contains the dependencies of the dependant (master record ids and vtags)
        fwdEntry.addData(DEPENDENCIES_KEY, this.serializationUtil.serializeDependenciesForward(newDependencies));

        return fwdEntry;
    }

    private IndexEntry createBackwardEntry(RecordId dependency, RecordId dependantRecordId, SchemaId dependantVtagId,
                                           Set<SchemaId> fields, Set<String> moreDimensionedVariantProperties)
            throws IOException {

        final byte[] serializedVariantPropertiesPattern = this.serializationUtil.serializeVariantPropertiesPattern(
                this.serializationUtil.createVariantPropertiesPattern(dependency.getVariantProperties(),
                        moreDimensionedVariantProperties));


        final IndexEntry bwdEntry = new IndexEntry(backwardDerefIndex.getDefinition());
        bwdEntry.addField("dependency_masterrecordid", dependency.getMaster().toBytes());
        bwdEntry.addField("dependant_vtag", dependantVtagId.getBytes());
        bwdEntry.addField("variant_properties_pattern", serializedVariantPropertiesPattern);

        // the identifier is the dependant which depends on the dependency
        bwdEntry.setIdentifier(dependantRecordId.toBytes());

        // the fields which the dependant uses of the dependency (null if used for deleting the entry)
        if (fields != null)
            bwdEntry.addData(FIELDS_KEY, this.serializationUtil.serializeFields(fields));

        return bwdEntry;
    }

    /**
     * Find the set of record ids (and corresponding version tags) on which a given record (in a given version tag)
     * depends.
     *
     * @param recordId record id of the record to find dependencies for
     * @param vtag     vtag of the record to find dependencies for
     * @return the record ids and vtags on which the given record depends
     */
    Set<DependencyEntry> findDependencies(RecordId recordId, SchemaId vtag) throws IOException {
        final Query query = new Query();
        query.addEqualsCondition("dependant_recordid", recordId.toBytes());
        query.addEqualsCondition("dependant_vtag", vtag.getBytes());

        final Set<DependencyEntry> result;

        final QueryResult queryResult = forwardDerefIndex.performQuery(query);
        if (queryResult.next() != null) {
            final byte[] serializedEntries = queryResult.getData(DEPENDENCIES_KEY);
            result = this.serializationUtil.deserializeDependenciesForward(serializedEntries);

            if (queryResult.next() != null) {
                throw new IllegalStateException(
                        "Expected only a single matching entry in " + forwardDerefIndex.getDefinition().getName());
            }

        } else {
            result = new HashSet<DependencyEntry>();
        }

        // Not closed in finally block: avoid HBase contact when there could be connection problems.
        Closer.close(queryResult);

        return result;
    }

    @Override
    public DependantRecordIdsIterator findDependantsOf(RecordId dependency, Set<SchemaId> fields,
                                                       SchemaId vtag) throws IOException {
        final RecordId master = dependency.getMaster();

        final Query query = new Query();
        query.addEqualsCondition("dependency_masterrecordid", master.toBytes());
        if (vtag != null)
            query.addEqualsCondition("dependant_vtag", vtag.getBytes());

        query.setIndexFilter(new DerefMapIndexFilter(dependency.getVariantProperties(), fields));

        return new DependantRecordIdsIteratorImpl(backwardDerefIndex.performQuery(query), this.serializationUtil);
    }

    @Override
    public DependantRecordIdsIterator findDependantsOf(RecordId dependency, SchemaId field,
                                                       SchemaId vtag) throws IOException {
        return findDependantsOf(dependency, field == null ? null : Sets.newHashSet(field), vtag);
    }

    @Override
    public DependantRecordIdsIterator findDependantsOf(RecordId dependency) throws IOException {
        return findDependantsOf(dependency, (Set<SchemaId>) null, null);
    }

}
