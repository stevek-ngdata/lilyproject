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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotometrics.orderly.StructIterator;
import com.gotometrics.orderly.StructRowKey;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.lilyproject.hbaseindex.IndexDefinition;
import org.lilyproject.hbaseindex.IndexFieldDefinition;
import org.lilyproject.hbaseindex.IndexFilterHbaseImplProto;

import java.io.*;
import java.util.List;

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

    public IndexFilterHbaseImpl(byte[] indexFilterByteArray, byte[] indexDefinitionByteArray) {
        ByteArrayInputStream indexFilterByteArrayStream = new ByteArrayInputStream(indexFilterByteArray);
        DataInputStream indexFilterDataInputStream = new DataInputStream(indexFilterByteArrayStream);

        try {
            final String indexFilterClassName = indexFilterDataInputStream.readUTF();
            this.indexFilter = (IndexFilter) tryInstantiateClass(indexFilterClassName);
            this.indexFilter.readFields(indexFilterDataInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteArrayInputStream indexDefitionByteArrayStream = new ByteArrayInputStream(indexDefinitionByteArray);
        DataInputStream indexDefinitionDataInputStream = new DataInputStream(indexDefitionByteArrayStream);
        try {
            final String indexDefinitionClassName = indexDefinitionDataInputStream.readUTF();
            this.indexDefinition = (IndexDefinition) tryInstantiateClass(indexDefinitionClassName);
            this.indexDefinition.readFields(indexDefinitionDataInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IndexFilterHbaseImpl() {
        // for hbase readFields
    }

    @Override
    public ReturnCode filterKeyValue(Cell ignore) {
        KeyValue keyValue = (KeyValue) ignore;
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

   private IndexFilterHbaseImplProto.IndexFilterHbaseImpl convert() throws IOException {
        IndexFilterHbaseImplProto.IndexFilterHbaseImpl.Builder builder =
                IndexFilterHbaseImplProto.IndexFilterHbaseImpl.newBuilder();
        if (this.indexFilter != null) {
            ByteArrayOutputStream indexFilterByteArray = new ByteArrayOutputStream();
            DataOutput indexFilterDataOutput = new DataOutputStream(indexFilterByteArray);
            indexFilterDataOutput.writeUTF(this.indexFilter.getClass().getName());
            this.indexFilter.write(indexFilterDataOutput);
            builder.setIndexFilter(ByteString.copyFrom(indexFilterByteArray.toByteArray()));
        }
        if (this.indexDefinition != null) {
            ByteArrayOutputStream indexDefinitionByteArray = new ByteArrayOutputStream();
            DataOutput indexDefinitionDataOutput = new DataOutputStream(indexDefinitionByteArray);
            indexDefinitionDataOutput.writeUTF(this.indexDefinition.getClass().getName());
            this.indexDefinition.write(indexDefinitionDataOutput);
            builder.setIndexDefinition(ByteString.copyFrom(indexDefinitionByteArray.toByteArray()));
        }

        return builder.build();
    }

    public byte[] toByteArray() throws IOException { return convert().toByteArray(); }

    public static IndexFilterHbaseImpl parseFrom(final byte[] pbBytes) throws DeserializationException {
        IndexFilterHbaseImplProto.IndexFilterHbaseImpl proto;
        try {
            proto = IndexFilterHbaseImplProto.IndexFilterHbaseImpl.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        byte[] indexFilter = proto.getIndexFilter().toByteArray();
        byte[] indexDefinition = proto.getIndexDefinition().toByteArray();
        IndexFilterHbaseImpl newIndexFilterHbaseImpl = new IndexFilterHbaseImpl(indexFilter,indexDefinition);

        return newIndexFilterHbaseImpl;
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
