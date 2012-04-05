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
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.impl.FieldTypeImpl;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.util.Arrays;

import static org.lilyproject.util.hbase.LilyHBaseSchema.DELETE_MARKER;
import static org.lilyproject.util.hbase.LilyHBaseSchema.EXISTS_FLAG;
import static org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;

public class HBaseFieldValueFilter implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, Repository repository, HBaseRecordFilterFactory factory)
            throws RepositoryException, InterruptedException {
        
        if (!(uncastFilter instanceof FieldValueFilter)) {
            return null;
        }
        
        FieldValueFilter filter = (FieldValueFilter)uncastFilter;
        
        if (filter.getField() == null) {
            throw new IllegalArgumentException("Field name should be specified in FieldValueFilter");
        }
        
        if (filter.getFieldValue() == null) {
            throw new IllegalArgumentException("Field value should be specified in FieldValueFilter");
        }

        CompareOp compareOp = filter.getCompareOp() != null ? filter.getCompareOp() : CompareOp.EQUAL;
        if (compareOp != CompareOp.EQUAL && compareOp != CompareOp.NOT_EQUAL) {
            throw new IllegalArgumentException("FieldValueFilter does not support this compare operator: " + compareOp);
        }

        FieldType fieldType = repository.getTypeManager().getFieldTypeByName(filter.getField());
        DataOutput dataOutput = new DataOutputImpl();
        dataOutput.writeByte(EXISTS_FLAG);
        fieldType.getValueType().write(filter.getFieldValue(), dataOutput, new IdentityRecordStack());
        byte[] fieldValue = dataOutput.toByteArray();

        SingleColumnValueFilter hbaseFilter = new SingleColumnValueFilter(RecordCf.DATA.bytes,
                ((FieldTypeImpl)fieldType).getQualifier(), HBaseRecordFilterUtil.translateCompareOp(compareOp), fieldValue);
        hbaseFilter.setFilterIfMissing(filter.getFilterIfMissing());

        if (compareOp == CompareOp.NOT_EQUAL && filter.getFilterIfMissing()) {
            // In some cases delete markers get written rather than really deleting the field. Such delete markers
            // are also not equal to the searched field, therefore in case of the not equal operator we need to add
            // and extra filter to exclude the delete marker matches, except if filterIfMissing would be false.
            FilterList list = new FilterList();
            list.addFilter(new SingleColumnValueFilter(RecordCf.DATA.bytes,
                    ((FieldTypeImpl)fieldType).getQualifier(), CompareFilter.CompareOp.NOT_EQUAL, DELETE_MARKER));
            list.addFilter(hbaseFilter);
            return list;
        } else {
            return hbaseFilter;
        }
    }
}
