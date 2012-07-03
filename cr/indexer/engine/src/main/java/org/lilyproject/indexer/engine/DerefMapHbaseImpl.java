package org.lilyproject.indexer.engine;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.StructBuilder;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.Termination;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.io.Closer;

/**
 * @author Jan Van Besien
 */
public class DerefMapHbaseImpl implements DerefMap {

    private static final byte[] ENTRIES_KEY = Bytes.toBytes("entries");
    private static final byte[] FIELDS_KEY = Bytes.toBytes("fields");

    private final static int SCHEMA_ID_BYTE_LENGTH = 16; // see SchemaIdImpl

    private static final byte[] DUMMY_IDENTIFIER = new byte[]{0};


    private Index forwardDerefIndex;

    private Index backwardDerefIndex;

    private IdGenerator idGenerator;

    // TODO: good idea? construct with name of the index? it also means the tables are created when first used?
    // TODO: in general, need to think about how this will be instanciated and managed..
    public DerefMapHbaseImpl(final String indexName, final IndexManager indexManager, final IdGenerator idGenerator)
            throws IndexNotFoundException, IOException,
            InterruptedException {
//        metrics = new LinkIndexMetrics("linkIndex");

        this.idGenerator = idGenerator;

        {
            IndexDefinition indexDef = new IndexDefinition("deref-forward-" + indexName);
            // For the record ID we use a variable length byte array field of which the first two bytes are fixed length
            // The first byte is actually the record identifier byte.
            // The second byte really is the first byte of the record id. We put this in the fixed length part
            // (safely because a record id should at least be a single byte long) because this prevents BCD encoding
            // on the first byte, thus making it easier to configure table splitting based on the original input.
            indexDef.addVariableLengthByteField("dependant_recordid", 2);
            indexDef.addByteField("dependant_vtag", SCHEMA_ID_BYTE_LENGTH);
            forwardDerefIndex = indexManager.getIndex(indexDef);
        }

        {
            IndexDefinition indexDef = new IndexDefinition("deref-backward-" + indexName);
            // Same remark as in the forward index.
            indexDef.addVariableLengthByteField("depending_recordid", 2);
            indexDef.addByteField("depending_vtag", SCHEMA_ID_BYTE_LENGTH);
            backwardDerefIndex = indexManager.getIndex(indexDef);
        }
    }


    @Override
    public void updateDependencies(RecordId dependantRecordId, SchemaId dependantVtagId,
                                   Multimap<Entry, SchemaId> newDependencies) throws IOException {
        final Set<Entry> existingEntries = findDependencies(dependantRecordId, dependantVtagId);

        // Figure out what changed
        final Set<Entry> removedEntries = figureOutRemovedEntries(newDependencies, existingEntries);
        final Set<Entry> addedEntries = figureOutAddedEntries(newDependencies, existingEntries);

        // IMPORTANT implementation note: the order in which changes are applied is not arbitrary. It is such that if
        // the process would fail in between, there will never be left any state in the backward index which would not
        // be found via the forward index.


        // delete removed from bwd index
        for (Entry removedEntry : removedEntries) {
            final IndexEntry backwardEntry =
                    createBackwardEntry(removedEntry.getRecordId(), removedEntry.getVtag(), dependantRecordId,
                            newDependencies.get(removedEntry));
            backwardDerefIndex.removeEntry(backwardEntry);
        }

        // update fwd index (added and removed at the same time, it is a single row)
        final IndexEntry fwdEntry = createForwardEntry(dependantRecordId, dependantVtagId, newDependencies);
        forwardDerefIndex.addEntry(fwdEntry);

        // add added to bwd idx
        for (Entry addedEntry : addedEntries) {
            final IndexEntry backwardEntry =
                    createBackwardEntry(addedEntry.getRecordId(), addedEntry.getVtag(), dependantRecordId,
                            newDependencies.get(addedEntry));
            backwardDerefIndex.addEntry(backwardEntry);
        }
    }

    private Set<Entry> figureOutRemovedEntries(Multimap<Entry, SchemaId> newDependencies,
                                               Set<Entry> existingEntries) {
        final Set<Entry> removedEntries = new HashSet<Entry>(existingEntries);
        removedEntries.removeAll(newDependencies.keys());
        return removedEntries;
    }

    private Set<Entry> figureOutAddedEntries(Multimap<Entry, SchemaId> newDependencies,
                                             Set<Entry> existingEntries) {
        final Set<Entry> addedEntries = new HashSet<Entry>(newDependencies.keys());
        addedEntries.removeAll(existingEntries);
        return addedEntries;
    }

    private IndexEntry createForwardEntry(RecordId dependantRecordId, SchemaId dependantVtagId,
                                          Multimap<Entry, SchemaId> newDependencies) throws IOException {
        final IndexEntry fwdEntry = new IndexEntry(forwardDerefIndex.getDefinition());
        fwdEntry.addField("dependant_recordid", dependantRecordId.toBytes());
        fwdEntry.addField("dependant_vtag", dependantVtagId.getBytes());

        // we do not really use the identifier... all we are interested in is in the data of the entry
        fwdEntry.setIdentifier(DUMMY_IDENTIFIER);

        // the data contains the dependencies of the dependant (record ids and vtags)
        fwdEntry.addData(ENTRIES_KEY, serializeEntries(newDependencies.keySet()));

        return fwdEntry;
    }

    private IndexEntry createBackwardEntry(RecordId dependingRecordId, SchemaId dependingVtagId,
                                           RecordId dependantRecordId, Collection<SchemaId> fields) {
        final IndexEntry bwdEntry = new IndexEntry(backwardDerefIndex.getDefinition());
        bwdEntry.addField("depending_recordid", dependingRecordId.toBytes());
        bwdEntry.addField("depending_vtag", dependingVtagId.getBytes());

        // the identifier is the dependant which depends on the depending record
        bwdEntry.setIdentifier(dependantRecordId.toBytes());

        // the data contains the fields via which the dependant depends on the depending record
        bwdEntry.addData(FIELDS_KEY, serializeFields(fields));

        return bwdEntry;
    }

    /**
     * Serializes a list of {@link org.lilyproject.indexer.engine.DerefMap.Entry}s into a byte array. It uses a
     * variable length byte array encoding schema.
     *
     * @param entries list of entries to serialize
     * @return byte array with the serialized format
     */
    byte[] serializeEntries(Set<Entry> entries) throws IOException {
        final StructRowKey singleEntryRowKey = entrySerializationRowKey();

        // calculate length
        int totalLength = 0;
        final HashMap<Entry, Integer> entriesWithSerializedLength = new HashMap<Entry, Integer>();
        for (Entry entry : entries) {
            final int serializedLength = singleEntryRowKey
                    .getSerializedLength(new Object[]{entry.getRecordId().toBytes(), entry.getVtag().getBytes()});
            entriesWithSerializedLength.put(entry, serializedLength);
            totalLength += serializedLength;
        }

        // serialize
        final byte[] serialized = new byte[totalLength];
        int offset = 0;
        for (Map.Entry<Entry, Integer> mapEntry : entriesWithSerializedLength.entrySet()) {
            final Entry entry = mapEntry.getKey();
            singleEntryRowKey
                    .serialize(new Object[]{entry.getRecordId().toBytes(), entry.getVtag().getBytes()}, serialized,
                            offset);
            final Integer length = mapEntry.getValue();
            offset += length;
        }

        return serialized;
    }


    public Set<Entry> deserializeEntries(byte[] serialized) throws IOException {
        final StructRowKey singleEntryRowKey = entrySerializationRowKey();

        final HashSet<Entry> result = new HashSet<Entry>();

        final ImmutableBytesWritable bw = new ImmutableBytesWritable(serialized);

        while (bw.getSize() > 0) {
            final Object[] deserializedEntry = (Object[]) singleEntryRowKey.deserialize(bw);
            result.add(new Entry(idGenerator.fromBytes((byte[]) deserializedEntry[0]),
                    idGenerator.getSchemaId((byte[]) deserializedEntry[1])));
        }
        return result;
    }

    private StructRowKey entrySerializationRowKey() {
        final StructRowKey singleEntryRowKey = new StructBuilder().add(new VariableLengthByteArrayRowKey())
                .add(new FixedByteArrayRowKey(SCHEMA_ID_BYTE_LENGTH)).toRowKey();
        singleEntryRowKey.setTermination(Termination.MUST);
        return singleEntryRowKey;
    }

    /**
     * Serializes a list of field ids into a byte array. Each field id has a fixed length, thus the serialization
     * simply appends all the field ids byte representations.
     *
     * @param fields list of field ids to serialize
     * @return byte array containing all the byte representations of the field ids
     */
    byte[] serializeFields(Collection<SchemaId> fields) {
        final byte[] serialized = new byte[SCHEMA_ID_BYTE_LENGTH * fields.size()];
        final Iterator<SchemaId> iterator = fields.iterator();
        int idx = 0;
        while (iterator.hasNext()) {
            final byte[] bytes = iterator.next().getBytes();
            assert SCHEMA_ID_BYTE_LENGTH == bytes.length;
            System.arraycopy(bytes, 0, serialized, idx * SCHEMA_ID_BYTE_LENGTH, SCHEMA_ID_BYTE_LENGTH);
            idx++;
        }

        return serialized;
    }

    Set<SchemaId> deserializeFields(byte[] serialized) {
        final HashSet<SchemaId> result = new HashSet<SchemaId>();
        for (int i = 0; i < serialized.length; i += SCHEMA_ID_BYTE_LENGTH) {
            byte[] bytes = new byte[SCHEMA_ID_BYTE_LENGTH];
            System.arraycopy(serialized, i, bytes, 0, SCHEMA_ID_BYTE_LENGTH);
            result.add(idGenerator.getSchemaId(bytes));
        }
        return result;
    }

    @Override
    public Set<Entry> findDependencies(RecordId recordId, SchemaId vtag) throws IOException {
        final Query query = new Query();
        query.addEqualsCondition("dependant_recordid", recordId.toBytes());
        query.addEqualsCondition("dependant_vtag", vtag.getBytes());

        final Set<Entry> result;

        final QueryResult queryResult = forwardDerefIndex.performQuery(query);
        if (queryResult.next() != null) {
            final byte[] serializedEntries = queryResult.getData(ENTRIES_KEY);
            result = deserializeEntries(serializedEntries);

            if (queryResult.next() != null) {
                throw new IllegalStateException(
                        "Expected only a single matching entry in " + forwardDerefIndex.getDefinition().getName());
            }

        } else {
            result = new HashSet<Entry>();
        }

        // Not closed in finally block: avoid HBase contact when there could be connection problems.
        Closer.close(queryResult);

        return result;
    }

    @Override
    public DependantRecordIdsIterator findDependantsOf(Entry recordEntry, SchemaId field) throws IOException {
        final Query query = new Query();
        query.addEqualsCondition("depending_recordid", recordEntry.getRecordId().toBytes());
        query.addEqualsCondition("depending_vtag", recordEntry.getVtag().getBytes());

        final QueryResult queryResult = backwardDerefIndex.performQuery(query);
        return new DependantRecordIdsIteratorImpl(queryResult, field);
    }

    final class DependantRecordIdsIteratorImpl implements DependantRecordIdsIterator {
        final QueryResult queryResult;
        final SchemaId queriedField;

        DependantRecordIdsIteratorImpl(QueryResult queryResult, SchemaId queriedField) {
            ArgumentValidator.notNull(queryResult, "queryResult");
            ArgumentValidator.notNull(queriedField, "queriedField");

            this.queryResult = queryResult;
            this.queriedField = queriedField;
        }

        @Override
        public void close() throws IOException {
            queryResult.close();
        }

        RecordId next = null;

        private RecordId getNextFromQueryResult() throws IOException {
            byte[] nextIdentifier = null;
            while ((nextIdentifier = queryResult.next()) != null) {
                // the identifier is the record id of the record that depends on the queried record
                // but we only include it if the dependency is via the specified field

                // TODO: we could optimize the implementation somehow to make this filtering happen on region server... (but how to integrate that with hbase index library in a generic fashion?)

                final Set<SchemaId> dependingFields = deserializeFields(queryResult.getData(FIELDS_KEY));
                if (dependingFields.contains(queriedField)) {
                    return idGenerator.fromBytes(nextIdentifier);
                }
            }

            return null; // query result exhausted
        }

        @Override
        public boolean hasNext() throws IOException {
            synchronized (this) { // to protect setting/resetting the next value from race conditions
                if (next != null) {
                    // the next was already set, but not yet used
                    return true;
                } else {
                    // try setting a next value
                    next = getNextFromQueryResult();
                    return next != null;
                }
            }
        }

        @Override
        public RecordId next() throws IOException {
            synchronized (this) { // to protect setting/resetting the next value from race conditions
                if (next != null) {
                    // the next was already set, but not yet used
                    RecordId nextToReturn = next;
                    next = null;
                    return nextToReturn;
                } else {
                    // try setting a next value
                    next = getNextFromQueryResult();
                    return next;
                }
            }
        }

    }
}
