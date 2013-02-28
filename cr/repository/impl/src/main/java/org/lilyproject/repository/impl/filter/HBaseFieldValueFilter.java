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
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.CompareOp;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.impl.FieldTypeImpl;
import org.lilyproject.repository.impl.hbase.LilyFieldSingleColumnValueFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;

public class HBaseFieldValueFilter implements HBaseRecordFilterFactory {
    @Override
    public Filter createHBaseFilter(RecordFilter uncastFilter, RepositoryManager repositoryManager, HBaseRecordFilterFactory factory)
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

        FieldType fieldType = repositoryManager.getTypeManager().getFieldTypeByName(filter.getField());
        DataOutput dataOutput = new DataOutputImpl();
        fieldType.getValueType().write(filter.getFieldValue(), dataOutput, new IdentityRecordStack());
        byte[] fieldValue = dataOutput.toByteArray();

        LilyFieldSingleColumnValueFilter hbaseFilter = new LilyFieldSingleColumnValueFilter(RecordCf.DATA.bytes,
                ((FieldTypeImpl)fieldType).getQualifier(), HBaseRecordFilterUtil.translateCompareOp(compareOp), fieldValue);
        hbaseFilter.setFilterIfMissing(filter.getFilterIfMissing());

        return hbaseFilter;
    }
}
