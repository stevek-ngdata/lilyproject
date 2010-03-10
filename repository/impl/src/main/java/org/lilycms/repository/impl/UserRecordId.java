/*
 * Copyright 2010 Outerthought bvba
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
package org.lilycms.repository.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.RecordId;


public class UserRecordId implements RecordId {

    protected final String recordIdString;
    protected byte[] recordIdBytes;
    private final IdGenerator idGenerator;

    public UserRecordId(String recordId, IdGenerator idGenerator) {
        this.recordIdString = recordId;
        recordIdBytes = Bytes.toBytes(recordId);
        this.idGenerator = idGenerator;
    }

    public UserRecordId(byte[] recordId, IdGenerator idGenerator) {
        recordIdBytes = recordId;
        recordIdString = Bytes.toString(recordId);
        this.idGenerator = idGenerator;
    }

    public byte[] toBytes() {
        return idGenerator.toBytes(this);
    }

    public String toString() {
        return idGenerator.toString(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((recordIdString == null) ? 0 : recordIdString.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UserRecordId other = (UserRecordId) obj;
        if (recordIdString == null) {
            if (other.recordIdString != null)
                return false;
        } else if (!recordIdString.equals(other.recordIdString))
            return false;
        return true;
    }
}
