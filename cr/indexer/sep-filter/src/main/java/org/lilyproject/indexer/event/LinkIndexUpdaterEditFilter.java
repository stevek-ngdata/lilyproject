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
package org.lilyproject.indexer.event;

import java.util.List;

import com.ngdata.sep.WALEditFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

/**
 * Edit filter for the {@code LinkIndexUpdater}.
 */
public class LinkIndexUpdaterEditFilter implements WALEditFilter {

    @Override
    public void apply(WALEdit walEdit) {
        List<KeyValue> keyValues = walEdit.getKeyValues();
        for (int i = keyValues.size() - 1; i >= 0; i--) {
            KeyValue kv = keyValues.get(i);
            if (!(kv.matchingColumn(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes))) {
                keyValues.remove(i);
            }
        }
    }

}
