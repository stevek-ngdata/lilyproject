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

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class HBaseRecordTypeFilter implements HBaseRecordFilterFactory {

    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordTypeFilter)) {
            return null;
        }

        RecordTypeFilter filter = (RecordTypeFilter)uncastFilter;

        Filter nameFilter = null;
        Filter versionFilter = null;

        if (filter.getRecordType() != null) {
            RecordType recordType = repository.getTypeManager().getRecordTypeByName(filter.getRecordType(), null);
            nameFilter = new SingleColumnValueFilter(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_ID.bytes,
                    CompareFilter.CompareOp.EQUAL, recordType.getId().getBytes());
        }

        if (filter.getVersion() != null) {
            versionFilter = new SingleColumnValueFilter(RecordCf.DATA.bytes,
                    RecordColumn.NON_VERSIONED_RT_VERSION.bytes, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(filter.getVersion()));
        }

        Filter result;
        if (nameFilter == null && versionFilter == null) {
            throw new IllegalArgumentException("A RecordTypeFilter should at least specify the record type or its version.");
        } else if (nameFilter != null && versionFilter != null) {
            FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            list.addFilter(nameFilter);
            list.addFilter(versionFilter);
            result = list;
        } else if (nameFilter != null) {
            result = nameFilter;
        } else if (versionFilter != null) {
            result = versionFilter;
        } else {
            throw new RuntimeException("Expected this to be unreachable.");
        }

        return result;
    }
}
