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
package org.lilyproject.repository.impl;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.RecordId;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;


public class UserRecordId implements RecordId {

    protected final String basicRecordIdString;
    protected byte[] recordIdBytes;
    protected String recordIdString;
    private final IdGeneratorImpl idGenerator;
    
    private static final SortedMap<String, String> EMPTY_SORTED_MAP = Collections.unmodifiableSortedMap(new TreeMap<String, String>());

    protected UserRecordId(String recordId, IdGeneratorImpl idGenerator) {
        IdGeneratorImpl.checkIdString(recordId, "record id");
        this.basicRecordIdString = recordId;
        this.idGenerator = idGenerator;
    }

    protected UserRecordId(DataInput dataInput, IdGeneratorImpl idGenerator) {
        basicRecordIdString = dataInput.readUTF();
        IdGeneratorImpl.checkIdString(basicRecordIdString, "record id");
        this.idGenerator = idGenerator;
    }

    @Override
    public byte[] toBytes() {
        if (recordIdBytes == null) {
            recordIdBytes = idGenerator.toBytes(this);
        }
        return recordIdBytes;
    }
    
    @Override
    public void writeBytes(DataOutput dataOutput) {
        if (recordIdBytes == null) {
            idGenerator.writeBytes(this, dataOutput);
        } else {
            dataOutput.writeBytes(recordIdBytes);
        }
    }

    public String toString() {
        if (recordIdString == null) {
            recordIdString = idGenerator.toString(this);
        }
        return recordIdString;
    }
    
    /**
     * Writes the byte representation of the user record id to the DataOutput, without adding the identifying byte
     */

    public void writeBasicBytes(DataOutput dataOutput) {
        dataOutput.writeUTF(basicRecordIdString);
    }
    
    protected String getBasicString() {
        return basicRecordIdString;
    }

    @Override
    public SortedMap<String, String> getVariantProperties() {
        return EMPTY_SORTED_MAP;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((basicRecordIdString == null) ? 0 : basicRecordIdString.hashCode());
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
        if (basicRecordIdString == null) {
            if (other.basicRecordIdString != null)
                return false;
        } else if (!basicRecordIdString.equals(other.basicRecordIdString))
            return false;
        return true;
    }

    @Override
    public RecordId getMaster() {
        return this;
    }

    @Override
    public boolean isMaster() {
        return true;
    }
}
