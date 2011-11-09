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

import java.util.*;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.RecordId;

public class UUIDRecordId implements RecordId {

    private UUID uuid;
    private String basicUUIDString;
    private String uuidString;
    private byte[] uuidBytes;
    private final IdGeneratorImpl idGenerator;

    private static final SortedMap<String, String> EMPTY_SORTED_MAP = Collections.unmodifiableSortedMap(new TreeMap<String, String>());

    protected UUIDRecordId(IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
        uuid = UUID.randomUUID();
    }
    
    protected UUIDRecordId(DataInput dataInput, IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
        this.uuid = new UUID(dataInput.readLong(), dataInput.readLong());
    }

    public UUIDRecordId(String basicUUIDString, IdGeneratorImpl idgenerator) {
        this.idGenerator = idgenerator;
        this.uuid = UUID.fromString(basicUUIDString);
        this.basicUUIDString = basicUUIDString;
    }
    
    public UUID getUuid() {
        return uuid;
    }
    
    public String toString() {
        if (uuidString == null) {
            uuidString = idGenerator.toString(this);
        }
        return uuidString;
    }
    
    public byte[] toBytes() {
        if (uuidBytes == null) {
            uuidBytes = idGenerator.toBytes(this);
        }
        return uuidBytes;
    }
    
    public void writeBytes(DataOutput dataOutput) {
        if (uuidBytes == null) {
            idGenerator.writeBytes(this, dataOutput);
        } else {
            dataOutput.writeBytes(uuidBytes);
        }
    }

    public SortedMap<String, String> getVariantProperties() {
        return EMPTY_SORTED_MAP;
    }

    /**
     * Writes the byte representation of the uuid to the DataOutput, without adding the identifying byte.
     */
    public void writeBasicBytes(DataOutput dataOutput) {
        dataOutput.writeLong(uuid.getMostSignificantBits());
        dataOutput.writeLong(uuid.getLeastSignificantBits());
    }
    
    protected String getBasicString() {
        if (basicUUIDString == null) {
            basicUUIDString = uuid.toString();
        }
        return basicUUIDString;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
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
        UUIDRecordId other = (UUIDRecordId) obj;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        return true;
    }

    public RecordId getMaster() {
        return this;
    }

    public boolean isMaster() {
        return true;
    }
}
