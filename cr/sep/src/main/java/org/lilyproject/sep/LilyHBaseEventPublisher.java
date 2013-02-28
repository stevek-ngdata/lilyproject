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
package org.lilyproject.sep;

import java.io.IOException;

import com.ngdata.sep.impl.HBaseEventPublisher;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

/**
 * Ensures that events are only published to the record table if the record for which the event is to be published exists
 * and is not deleted.
 */
public class LilyHBaseEventPublisher extends HBaseEventPublisher {

    private static final byte[] FALSE_BYTES = Bytes.toBytes(false);

    public LilyHBaseEventPublisher(HTableInterface recordTable) {
        super(recordTable, LilyHBaseSchema.RecordCf.DATA.bytes, LilyHBaseSchema.RecordColumn.PAYLOAD.bytes);
    }

    @Override
    public void publishEvent(byte[] row, byte[] payload) throws IOException {
        Put messagePut = new Put(row);
        messagePut.add(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, 1L, payload);
        getPayloadTable().checkAndPut(row, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, FALSE_BYTES, messagePut);
    }

}
