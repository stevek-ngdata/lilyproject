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
import org.apache.hadoop.hbase.filter.FilterList;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

public class HBaseRecordFilterList implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, LRepository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {

        if (!(uncastFilter instanceof RecordFilterList)) {
            return null;
        }

        RecordFilterList filter = (RecordFilterList)uncastFilter;

        FilterList.Operator hbaseOp = filter.getOperator() == RecordFilterList.Operator.MUST_PASS_ONE ?
                FilterList.Operator.MUST_PASS_ONE : FilterList.Operator.MUST_PASS_ALL;

        FilterList hbaseFilter = new FilterList(hbaseOp);

        for (RecordFilter subFilter : filter.getFilters()) {
            hbaseFilter.addFilter(factory.createHBaseFilter(subFilter, repository, factory));
        }

        return hbaseFilter;
    }
}
