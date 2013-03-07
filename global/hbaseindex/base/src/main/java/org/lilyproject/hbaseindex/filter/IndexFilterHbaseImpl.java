/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.hbaseindex.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.gotometrics.orderly.StructIterator;
import com.gotometrics.orderly.StructRowKey;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.lilyproject.hbaseindex.IndexDefinition;
import org.lilyproject.hbaseindex.IndexFieldDefinition;

/**
 * Actual implementation of {@link IndexFilter} as an HBase filter.
 *
 *
 */
public class IndexFilterHbaseImpl extends FilterBase {

    private IndexFilter indexFilter;

    private IndexDefinition indexDefinition;

    public IndexFilterHbaseImpl(IndexFilter indexFilter, IndexDefinition indexDefinition) {
        this.indexFilter = indexFilter;
        this.indexDefinition = indexDefinition;
    }

    public IndexFilterHbaseImpl() {
        // for hbase readFields
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        // for all data qualifiers that we are interested in
        for (byte[] dataQualifier : indexFilter.getFilteredDataQualifiers()) {
            // check if the key value is about one of those data qualifiers
            if (keyValue.matchingColumn(IndexDefinition.DATA_FAMILY, dataQualifier)) {
                // if it is, apply the filter
                if (indexFilter.filterData(dataQualifier, keyValue.getBuffer(), keyValue.getValueOffset(),
                        keyValue.getValueLength())) {
                    return ReturnCode.NEXT_ROW; // this result is ignored
                }
            }
        }

        return ReturnCode.INCLUDE; // not skipped
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        final StructRowKey structRowKey = indexDefinition.asStructRowKey();
        structRowKey.iterateOver(buffer, offset);

        final StructIterator fieldsIterator = structRowKey.iterator();

        final List<IndexFieldDefinition> fieldDefinitions = indexDefinition.getFields();

        // for all defined field definitions
        for (IndexFieldDefinition field : fieldDefinitions) {
            // check if the field should be filtered
            if (indexFilter.getFields().contains(field.getName())) {
                final Object nextField = fieldsIterator.next();
                if (indexFilter.filterField(field.getName(), nextField)) {
                    return true; // this result is ignored
                }
            } else {
                try {
                    fieldsIterator.skip();
                } catch (IOException e) {
                    throw new RuntimeException("failed to skip, index inconsistency?", e);
                }
            }
        }

        return false; // nothing was  skipped
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(indexDefinition.getClass().getName());
        indexDefinition.write(out);

        out.writeUTF(indexFilter.getClass().getName());
        indexFilter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final String indexDefinitionClassName = in.readUTF();
        indexDefinition = (IndexDefinition) tryInstantiateClass(indexDefinitionClassName);
        indexDefinition.readFields(in);

        final String indexFilterClassName = in.readUTF();
        indexFilter = (IndexFilter) tryInstantiateClass(indexFilterClassName);
        indexFilter.readFields(in);
    }

    private Object tryInstantiateClass(String className) throws IOException {
        try {
            return Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

}
