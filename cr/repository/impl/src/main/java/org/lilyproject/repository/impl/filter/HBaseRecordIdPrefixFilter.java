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
package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

public class HBaseRecordIdPrefixFilter implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, RepositoryManager repositoryManager, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordIdPrefixFilter)) {
            return null;
        }

        RecordIdPrefixFilter filter = (RecordIdPrefixFilter) uncastFilter;

        if (filter.getRecordId() == null) {
            throw new IllegalArgumentException("Record ID should be specified in RecordIdPrefixFilter");
        }

        RecordId recordId = filter.getRecordId();
        byte[] rowKeyPrefix = recordId.toBytes();

        return new PrefixFilter(rowKeyPrefix);
    }
}
