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

import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class HBaseRecordTypeFilter implements HBaseRecordFilterFactory {

    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, LRepository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordTypeFilter)) {
            return null;
        }

        RecordTypeFilter filter = (RecordTypeFilter)uncastFilter;

        Filter result = null;

        if (filter.getRecordType() == null) {
            throw new IllegalArgumentException("A RecordTypeFilter should at least specify the record type name.");
        }

        RecordType recordType = repository.getTypeManager().getRecordTypeByName(filter.getRecordType(), null);

        RecordTypeFilter.Operator operator =
                filter.getOperator() != null ? filter.getOperator() : RecordTypeFilter.Operator.EQUALS;

        switch (operator) {
            case EQUALS:
                Filter nameFilter = createRecordTypeFilter(recordType.getId());

                Filter versionFilter = null;
                if (filter.getVersion() != null) {
                    versionFilter = new SingleColumnValueFilter(RecordCf.DATA.bytes,
                            RecordColumn.NON_VERSIONED_RT_VERSION.bytes, CompareFilter.CompareOp.EQUAL,
                            Bytes.toBytes(filter.getVersion()));
                }

                if (versionFilter == null) {
                    result = nameFilter;
                } else {
                    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    list.addFilter(nameFilter);
                    list.addFilter(versionFilter);
                    result = list;
                }

                break;
            case INSTANCE_OF:
                Set<SchemaId> subtypes = repository.getTypeManager().findSubtypes(recordType.getId());
                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                list.addFilter(createRecordTypeFilter(recordType.getId()));
                for (SchemaId subType : subtypes) {
                    list.addFilter(createRecordTypeFilter(subType));
                }
                result = list;
                break;
            default:
                throw new RuntimeException("Unexpected operator: " + filter.getOperator());
        }

        return result;
    }

    private Filter createRecordTypeFilter(SchemaId schemaId) {
        return new SingleColumnValueFilter(RecordCf.DATA.bytes, RecordColumn.NON_VERSIONED_RT_ID.bytes,
                                   CompareFilter.CompareOp.EQUAL, schemaId.getBytes());
    }
}
