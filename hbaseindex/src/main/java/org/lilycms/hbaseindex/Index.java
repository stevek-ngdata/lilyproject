package org.lilycms.hbaseindex;

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

    public Index(HTable htable, IndexDefinition definition) {
        this.htable = htable;
        this.definition = definition;
    }

    public void addEntry(IndexEntry entry, byte[] targetKey) throws IOException {
        // check entry does not conflict with definition
        // TODO

        // build index key

        // calculate size of the index key
        int keyLength = targetKey.length + getIndexKeyLength();

        byte[] indexKey = new byte[keyLength];

        // put data in the key
        int offset = 0;
        for (IndexFieldDefinition fieldDef : definition.getFields()) {
            Object value = entry.getValue(fieldDef.getName());
            offset = fieldDef.toBytes(indexKey, offset, value);
        }

        // put target key in the key
        System.arraycopy(targetKey, 0, indexKey, offset, targetKey.length);

        Put put = new Put(indexKey);
        put.add(DUMMY_FAMILY, DUMMY_QUALIFIER, DUMMY_VALUE);

        htable.put(put);
    }

    private int getIndexKeyLength() {
        int length = 0;
        for (IndexFieldDefinition fieldDef : definition.getFields()) {
            length += fieldDef.getByteLength();
        }
        return length;
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
            keyOffset = fieldDef.toBytes(fromKey, keyOffset, value);
        }

        System.arraycopy(fromKey, 0, toKey, 0, keyOffset);

        if (query.getRangeCondition() != null)  {
            Object fromValue = query.getRangeCondition().getFromValue();
            Object toValue = query.getRangeCondition().getToValue();

            IndexFieldDefinition fieldDef = fieldDefs.get(fieldDefs.size() - 1);
            fieldDef.toBytes(fromKey, keyOffset, fromValue);
            fieldDef.toBytes(toKey, keyOffset, toValue);
        }

        Scan scan = new Scan(fromKey);
//        scan.setFilter(new InclusiveStopFilter(toKey));
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(toKey)));
        return new ScannerQueryResult(htable.getScanner(scan), indexKeyLength);
    }
}
