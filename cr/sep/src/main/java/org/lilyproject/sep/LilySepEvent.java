/*
 * Copyright 2013 NGDATA nv
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
import java.util.List;

import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.util.repo.RecordEvent;

/**
 * A subclass of SepEvent providing some Lily specific information.
 *
 * <p>Extend your SEP listener from {@link LilyEventListener} to get these kinds of events.</p>
 */
public class LilySepEvent extends SepEvent {
    private IdGenerator idGenerator;
    private String lilyRepositoryName;
    private String lilyTableName;
    private RecordId recordId;
    private AbsoluteRecordId absRecordId;

    public LilySepEvent(IdGenerator idGenerator, String lilyRepositoryName, String lilyTableName, byte[] table,
            byte[] row, List<KeyValue> keyValues, byte[] payload) {
        super(table, row, keyValues, payload);
        this.idGenerator = idGenerator;
        this.lilyRepositoryName = lilyRepositoryName;
        this.lilyTableName = lilyTableName;
    }

    public String getLilyTableName() {
        return lilyTableName;
    }

    public String getLilyRepositoryName() {
        return lilyRepositoryName;
    }

    public RecordId getRecordId() {
        if (recordId == null) {
            recordId = idGenerator.fromBytes(getRow());
        }
        return recordId;
    }

    public AbsoluteRecordId getAbsoluteRecordId() {
        if (absRecordId == null) {
            absRecordId = idGenerator.newAbsoluteRecordId(getLilyTableName(), getRecordId());
        }
        return absRecordId;
    }

    public RecordEvent getRecordEvent() throws IOException {
        return new RecordEvent(getPayload(), idGenerator);
    }
}
