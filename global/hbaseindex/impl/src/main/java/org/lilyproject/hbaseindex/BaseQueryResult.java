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

import com.gotometrics.orderly.StructIterator;
import com.gotometrics.orderly.StructRowKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

abstract class BaseQueryResult implements QueryResult {
    protected IndexDefinition definition;
    protected Result currentResult;
    protected QueryResult currentQResult;

    protected BaseQueryResult(IndexDefinition definition) {
        this.definition = definition;
    }

    @Override
    public byte[] getData(byte[] qualifier) {
        if (currentResult != null) {
            return currentResult.getValue(IndexDefinition.DATA_FAMILY, qualifier);
        } else if (currentQResult != null) {
            return currentQResult.getData(qualifier);
        } else {
            throw new RuntimeException("QueryResult.getData() is being called but there is no current result.");
        }
    }

    @Override
    public byte[] getData(String qualifier) {
        return getData(Bytes.toBytes(qualifier));
    }

    @Override
    public String getDataAsString(String qualifier) {
        return Bytes.toString(getData(Bytes.toBytes(qualifier)));
    }

    @Override
    public Object getIndexField(String fieldName) throws IOException {
        if (currentResult != null) {
            return decodeIndexFieldFrom(fieldName, currentResult.getRow());
        } else if (currentQResult != null) {
            return currentQResult.getIndexField(fieldName);
        } else {
            throw new RuntimeException("QueryResult.getIndexField() is being called but there is no current result.");
        }
    }

    private Object decodeIndexFieldFrom(String fieldName, byte[] rowKey) throws IOException {
        final StructRowKey structRowKey = definition.asStructRowKey();
        structRowKey.iterateOver(rowKey);

        final StructIterator iterator = structRowKey.iterator();

        int fieldPosition = definition.getFieldPosition(fieldName);

        if (fieldPosition == -1)
            throw new MalformedQueryException("field [" + fieldName + "] is not part of the index");

        // skip all fields up to fieldPosition
        for (int i = 0; i < fieldPosition; i++) {
            iterator.skip();
        }

        // return the requested field
        return iterator.next();
    }

}
