package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

// TODO is this threadsafe or not
public class Index {
    private HTable htable;
    private IndexDefinition definition;

    private static final byte[] DUMMY_FAMILY = Bytes.toBytes("dummy");
    private static final byte[] DUMMY_QUALIFIER = Bytes.toBytes("dummy");
    private static final byte[] DUMMY_VALUE = Bytes.toBytes("dummy");

    /** Number of bytes overhead per field. */
    private static final int FIELD_OVERHEAD = 1;

    public Index(HTable htable, IndexDefinition definition) {
        this.htable = htable;
        this.definition = definition;
    }

    public void addEntry(IndexEntry entry, byte[] targetRowKey) throws IOException {
        // check entry does not conflict with definition
        // TODO

        byte[] indexKey = buildRowKey(entry, targetRowKey);
        Put put = new Put(indexKey);

        // HBase does not allow to create a row without columns, so add some dummy ones.
        put.add(DUMMY_FAMILY, DUMMY_QUALIFIER, DUMMY_VALUE);

        htable.put(put);
    }

    /**
     * Removes an entry from the index. The contents of the supplied
     * entry and the targetRowKey should exactly match those supplied
     * when creating the index entry.
     */
    public void removeEntry(IndexEntry entry, byte[] targetRowKey) throws IOException {
        // TODO verify entry
        byte[] indexKey = buildRowKey(entry, targetRowKey);
        Delete delete = new Delete(indexKey);
        htable.delete(delete);
    }

    /**
     * Build the index row key.
     *
     * <p>The format is as follows:
     *
     * <pre>
     * ([1 byte field flags][fixed length value as bytes])*[target key]
     * </pre>
     *
     * <p>The field flags are currently used to mark if a field is null
     * or not. If a field is null, its value will be encoded as all-zero bits.
     */
    private byte[] buildRowKey(IndexEntry entry, byte[] targetKey) {
        // calculate size of the index key
        int keyLength = targetKey.length + getIndexKeyLength();

        byte[] indexKey = new byte[keyLength];

        // put data in the key
        int offset = 0;
        for (IndexFieldDefinition fieldDef : definition.getFields()) {
            Object value = entry.getValue(fieldDef.getName());
            offset = putField(indexKey, offset, fieldDef, value);
        }

        // put target key in the key
        System.arraycopy(targetKey, 0, indexKey, offset, targetKey.length);

        return indexKey;
    }

    private int getIndexKeyLength() {
        int length = 0;
        for (IndexFieldDefinition fieldDef : definition.getFields()) {
            length += fieldDef.getByteLength() + FIELD_OVERHEAD;
        }
        return length;
    }

    private int putField(byte[] bytes, int offset, IndexFieldDefinition fieldDef, Object value) {
        if (value == null) {
            bytes[offset] = setNullFlag((byte)0);
        }
        offset++;

        if (value != null) {
            fieldDef.toBytes(bytes, offset, value);
        }
        offset += fieldDef.getByteLength();

        return offset;
    }

    private byte setNullFlag(byte flags) {
        return (byte)(flags | 0x01);
    }

    public QueryResult performQuery(Query query) throws IOException {
        // TODO validate that the query fields match the index def fields and that the values are of correct type

        // construct from and to keys
        int indexKeyLength = getIndexKeyLength();

        byte[] fromKey = new byte[indexKeyLength];
        byte[] toKey = new byte[indexKeyLength];

        List<IndexFieldDefinition> fieldDefs = definition.getFields();

        int keyOffset = 0;
        int fieldDefCount = query.getRangeCondition() == null ? fieldDefs.size() : fieldDefs.size() - 1;
        for (int i = 0; i < fieldDefCount; i++) {
            IndexFieldDefinition fieldDef = fieldDefs.get(i);

            Object value = query.getCondition(fieldDef.getName()).getValue();
            keyOffset = putField(fromKey, keyOffset, fieldDef, value);
        }

        System.arraycopy(fromKey, 0, toKey, 0, keyOffset);

        if (query.getRangeCondition() != null)  {
            Object fromValue = query.getRangeCondition().getFromValue();
            Object toValue = query.getRangeCondition().getToValue();

            IndexFieldDefinition fieldDef = fieldDefs.get(fieldDefs.size() - 1);
            putField(fromKey, keyOffset, fieldDef, fromValue);
            putField(toKey, keyOffset, fieldDef, toValue);
        }

        Scan scan = new Scan(fromKey);
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(toKey)));
        return new ScannerQueryResult(htable.getScanner(scan), indexKeyLength);
    }

}
