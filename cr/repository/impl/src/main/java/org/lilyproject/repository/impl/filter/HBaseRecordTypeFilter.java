package org.lilyproject.repository.impl.filter;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.RecordFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.spi.HBaseRecordFilterFactory;

import static org.lilyproject.util.hbase.LilyHBaseSchema.*;

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
