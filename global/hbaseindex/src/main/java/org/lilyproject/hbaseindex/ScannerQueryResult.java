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
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * A QueryResult on top of a HBase scanner.
 */
class ScannerQueryResult extends BaseQueryResult {
    private ResultScanner scanner;
    private IndexDefinition definition;

    public ScannerQueryResult(ResultScanner scanner, IndexDefinition definition) {
        this.scanner = scanner;
        this.definition = definition;
    }

    @Override
    public byte[] next() throws IOException {
        currentResult = scanner.next();
        if (currentResult == null) {
            return null;
        }

        byte[] rowKey = currentResult.getRow();

        return decodeIdentifierFrom(rowKey);
    }

    private byte[] decodeIdentifierFrom(byte[] rowKey) throws IOException {
        final StructRowKey structRowKey = definition.asStructRowKey();
        structRowKey.iterateOver(rowKey);

        final StructIterator iterator = structRowKey.iterator();

        int nbrFields = structRowKey.getFields().length;
        // ignore all but last field (i.e. the identifier)
        for (int i = 0; i < nbrFields - 1; i++) {
            iterator.skip();
        }

        // read the last field (i.e. the identifier)
        return (byte[]) iterator.next();
    }

    @Override
    public void close() {
        scanner.close();
    }
}
