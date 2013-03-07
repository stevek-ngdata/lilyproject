/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.hbaseindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StructBuilder;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.Termination;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.lilyproject.hbaseindex.filter.IndexFilterHbaseImpl;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.ByteArrayKey;

/**
 * Allows to query an index, and add entries to it or remove entries from it.
 *
 * <p>An Index instance can be obtained from {@link IndexManager#getIndex(String)}.
 */
public class Index {
    private HTableInterface htable;
    private IndexDefinition definition;

    private static final byte[] DUMMY_QUALIFIER = new byte[]{0};
    private static final byte[] DUMMY_VALUE = new byte[]{0};

    protected Index(HTableInterface htable, IndexDefinition definition) {
        this.htable = htable;
        this.definition = definition;
    }

    public IndexDefinition getDefinition() {
        return definition;
    }

    /**
     * Adds an entry to this index. See {@link IndexEntry} for more information.
     *
     * @param entry the values to be part of the index key, should correspond to the fields
     *              defined in the {@link IndexDefinition}
     */
    public void addEntry(IndexEntry entry) throws IOException {
        ArgumentValidator.notNull(entry, "entry");
        entry.validate();

        Put put = createAddEntryPut(entry);
        htable.put(put);
    }

    /**
     * Adds multiple entries to the index. Uses one HBase put so is more efficient
     * than adding each entry individually.
     */
    public void addEntries(List<IndexEntry> entries) throws IOException {
        List<Put> puts = new ArrayList<Put>();

        for (IndexEntry entry : entries) {
            entry.validate();
            Put put = createAddEntryPut(entry);
            puts.add(put);
        }

        htable.put(puts);
    }

    private Put createAddEntryPut(IndexEntry entry) throws IOException {
        byte[] indexKey = buildRowKey(entry);
        Put put = new Put(indexKey);

        Map<ByteArrayKey, byte[]> data = entry.getData();
        if (data.size() > 0) {
            for (Map.Entry<ByteArrayKey, byte[]> item : data.entrySet()) {
                put.add(IndexDefinition.DATA_FAMILY, item.getKey().getKey(), item.getValue());
            }
        } else {
            // HBase does not allow to create a row without columns, so add a dummy column
            put.add(IndexDefinition.DATA_FAMILY, DUMMY_QUALIFIER, DUMMY_VALUE);
        }
        return put;
    }

    /**
     * Removes an entry from the index. The contents of the supplied
     * entry and the identifier should exactly match those supplied
     * when creating the index entry.
     */
    public void removeEntry(IndexEntry entry) throws IOException {
        ArgumentValidator.notNull(entry, "entry");
        entry.validate();

        byte[] indexKey = buildRowKey(entry);
        Delete delete = new Delete(indexKey);
        htable.delete(delete);
    }

    public void removeEntries(List<IndexEntry> entries) throws IOException {
        ArgumentValidator.notNull(entries, "entries");

        List<Delete> deletes = new ArrayList<Delete>();
        for (IndexEntry entry : entries) {
            entry.validate();

            byte[] indexKey = buildRowKey(entry);
            Delete delete = new Delete(indexKey);
            deletes.add(delete);
        }

        htable.delete(deletes);
    }

    /**
     * Build the index row key.
     *
     * <p>The format is as follows:
     *
     * <pre>
     * ([encoded value][terminator for variable length fields])*[identifier]
     * </pre>
     */
    private byte[] buildRowKey(IndexEntry entry) throws IOException {
        final StructRowKey indexEntryRowKeySerializer = definition.asStructRowKey();

        return indexEntryRowKeySerializer.serialize(entry.getFieldValuesInSerializationOrder());
    }

    public QueryResult performQuery(Query query) throws IOException {
        validateQuery(query);

        final StructBuilder fromKeyStructBuilder = new StructBuilder();
        final StructBuilder toKeyStructBuilder = new StructBuilder();

        // Construct from and to keys

        final List<Object> fromKeyComponents = new ArrayList<Object>(definition.getFields().size());
        byte[] fromKey = null;
        byte[] toKey = null;

        final Query.RangeCondition rangeCond = query.getRangeCondition();
        boolean rangeCondSet = false;
        int usedConditionsCount = 0;
        int definedFieldsIndex = 0;

        // loop through all defined index fields, and see if they occur in the query
        for (; definedFieldsIndex < definition.getFields().size(); definedFieldsIndex++) {
            final IndexFieldDefinition fieldDef = definition.getFields().get(definedFieldsIndex);

            final Query.EqualsCondition eqCond = query.getCondition(fieldDef.getName());
            if (eqCond != null) {
                // there is an equality condition for this field
                checkQueryValueType(fieldDef, eqCond.getValue());
                final RowKey key = fieldDef.asRowKey();
                key.setTermination(Termination.MUST);
                fromKeyStructBuilder.add(key);
                fromKeyComponents.add(eqCond.getValue());
                usedConditionsCount++;
            } else if (rangeCond != null) {
                // no equality condition for this field, but there is a range condition
                if (!rangeCond.getName().equals(fieldDef.getName())) {
                    throw new MalformedQueryException("Query defines range condition on field " + rangeCond.getName() +
                            " but has no equals condition on field " + fieldDef.getName() +
                            " which comes earlier in the index definition.");
                }

                final List<Object> toKeyComponents = new ArrayList<Object>(fromKeyComponents.size() + 1);
                toKeyComponents.addAll(fromKeyComponents);
                for (RowKey rowKey : fromKeyStructBuilder.getFields()) {
                    toKeyStructBuilder.add(rowKey);
                }

                final Object fromValue = query.getRangeCondition().getFromValue();
                final Object toValue = query.getRangeCondition().getToValue();

                if (fromValue == Query.MIN_VALUE) {
                    // just leave of the value, a shorter key is smaller than anything else
                } else {
                    checkQueryValueType(fieldDef, fromValue);
                    fromKeyComponents.add(fromValue);
                    fromKeyStructBuilder.add(fieldDef.asRowKeyWithoutTermination());
                }

                if (toValue == Query.MAX_VALUE) {
                    // Searching to max value is equal to a prefix search (assumes always exclusive interval,
                    // since max value is bigger than anything else)
                    // So, append nothing to the search key.
                } else {
                    checkQueryValueType(fieldDef, toValue);
                    toKeyComponents.add(toValue);
                    toKeyStructBuilder.add(fieldDef.asRowKeyWithoutTermination());
                }

                final StructRowKey frk = fromKeyStructBuilder.toRowKey();
                fromKey = frk.serialize(fromKeyComponents.toArray());
                final StructRowKey trk = toKeyStructBuilder.toRowKey();
                toKey = trk.serialize(toKeyComponents.toArray());

                rangeCondSet = true;
                usedConditionsCount++;

                break;
            } else {
                // we're done
                break;
            }
        }

        // Check if we have used all conditions defined in the query
        if (definedFieldsIndex < definition.getFields().size() &&
                usedConditionsCount < query.getEqConditions().size() + (rangeCond != null ? 1 : 0)) {
            StringBuilder message = new StringBuilder();
            message.append("The query contains conditions on fields which either did not follow immediately on ");
            message.append(
                    "the previous equals condition or followed after a range condition on a field. The fields are: ");
            for (; definedFieldsIndex < definition.getFields().size(); definedFieldsIndex++) {
                IndexFieldDefinition fieldDef = definition.getFields().get(definedFieldsIndex);
                if (query.getCondition(fieldDef.getName()) != null) {
                    message.append(fieldDef.getName());
                } else if (rangeCond != null && rangeCond.getName().equals(fieldDef.getName())) {
                    message.append(fieldDef.getName());
                }
                message.append(" ");
            }
            throw new MalformedQueryException(message.toString());
        }

        if (!rangeCondSet) {
            // Construct fromKey/toKey for the case there were only equals conditions
            final StructRowKey rk = fromKeyStructBuilder.toRowKey();
            rk.setTermination(Termination.MUST);
            fromKey = rk.serialize(fromKeyComponents.toArray());
            toKey = fromKey;
        }

        Scan scan = new Scan(fromKey);

        // Query.MAX_VALUE is a value which should be larger than anything, so cannot be an inclusive upper bound
        // The importance of this is because for Query.MAX_VALUE, we do a prefix scan so the operator should be
        // CompareOp.LESS_OR_EQUAL
        boolean upperBoundInclusive =
                rangeCond != null && (rangeCond.isUpperBoundInclusive() || rangeCond.getToValue() == Query.MAX_VALUE);
        CompareOp op = rangeCondSet && !upperBoundInclusive ? CompareOp.LESS : CompareOp.LESS_OR_EQUAL;
        Filter toFilter = new RowFilter(op, new BinaryPrefixComparator(toKey));

        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (query.getIndexFilter() != null) {
            filters.addFilter(new IndexFilterHbaseImpl(query.getIndexFilter(), definition));
        }

        if (rangeCondSet && !rangeCond.isLowerBoundInclusive()) {
            // TODO: optimize the performance hit caused by the extra filter
            //  Once the greater filter on the fromKey returns true, it will remain true because
            //  row keys are sorted. The RowFilter will however keep doing the check again and again
            //  on each new row key. We need a new filter in HBase, something like the opposite of the
            //  WhileMatchFilter.
            filters.addFilter(new RowFilter(CompareOp.GREATER, new BinaryPrefixComparator(fromKey)));
            filters.addFilter(new WhileMatchFilter(toFilter));
        } else {
            filters.addFilter(new WhileMatchFilter(toFilter));
        }

        scan.setFilter(filters);
        scan.setCaching(30);

        return new ScannerQueryResult(htable.getScanner(scan), definition);
    }

    /**
     * Validates that all fields used in the query actually exist in the index definition.
     *
     * TODO: shouldn't we also validate that the requested sort order corresponds with the indexed order etc?
     *
     * @param query query to validate
     */
    private void validateQuery(Query query) {
        for (Query.EqualsCondition eqCond : query.getEqConditions()) {
            if (definition.getField(eqCond.getName()) == null) {
                throw new MalformedQueryException(
                        String.format("The query refers to a field which does not exist in this index: %1$s",
                                eqCond.getName()));
            }
        }
        if (query.getRangeCondition() != null && definition.getField(query.getRangeCondition().getName()) == null) {
            throw new MalformedQueryException(
                    String.format("The query refers to a field which does not exist in this index: %1$s",
                            query.getRangeCondition().getName()));
        }
    }

    private void checkQueryValueType(IndexFieldDefinition fieldDef, Object value) {
        if (value != null) {
            final RowKey rowKey = fieldDef.asRowKey();
            if (!rowKey.getDeserializedClass().isAssignableFrom(value.getClass())) {
                throw new MalformedQueryException("query for field " + fieldDef.getName() + " contains" +
                        " a value of an incorrect type. Expected: " + rowKey.getDeserializedClass() +
                        ", found: " + value.getClass().getName());
            }
        }
    }

}
