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
package org.lilyproject.indexer.derefmap;

import java.io.IOException;

import org.lilyproject.hbaseindex.QueryResult;
import org.lilyproject.repository.api.AbsoluteRecordId;

/**
 * Implementation of {@link org.lilyproject.indexer.derefmap.DependantRecordIdsIterator}.
 *
 *
 */
final class DependantRecordIdsIteratorImpl implements DependantRecordIdsIterator {
    private final QueryResult queryResult;
    private final DerefMapSerializationUtil serializationUtil;

    DependantRecordIdsIteratorImpl(QueryResult queryResult, DerefMapSerializationUtil serializationUtil) {
        this.queryResult = queryResult;
        this.serializationUtil = serializationUtil;
    }

    @Override
    public void close() throws IOException {
        queryResult.close();
    }

    AbsoluteRecordId next = null;

    private AbsoluteRecordId getNextFromQueryResult() throws IOException {
        // the identifier is the record id of the record that depends on the queried record

        final byte[] nextIdentifier = queryResult.next();
        if (nextIdentifier == null) {
            return null;
        } else {
            return serializationUtil.deserializeDependantRecordId(nextIdentifier);
        }
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
    public AbsoluteRecordId next() throws IOException {
        synchronized (this) { // to protect setting/resetting the next value from race conditions
            if (next != null) {
                // the next was already set, but not yet used
                AbsoluteRecordId nextToReturn = next;
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
