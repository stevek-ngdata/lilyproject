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
package org.lilyproject.repository.impl.id;

import java.util.*;

import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
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
    
    protected UUIDRecordId(UUID uuid, IdGeneratorImpl idGenerator) {
        this.uuid = uuid;
        this.idGenerator = idGenerator;
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
    
    @Override
    public byte[] toBytes() {
        if (uuidBytes == null) {
            DataOutput dataOutput = new DataOutputImpl(17);
            writeBytes(dataOutput);
            uuidBytes = dataOutput.toByteArray();
        }
        return uuidBytes;
    }
    
    @Override
    public void writeBytes(DataOutput dataOutput) {
        if (uuidBytes == null) {
            dataOutput.writeByte(IdGeneratorImpl.IdType.UUID.getIdentifierByte());
            dataOutput.writeLong(uuid.getMostSignificantBits());
            dataOutput.writeLong(uuid.getLeastSignificantBits());
        } else {
            dataOutput.writeBytes(uuidBytes);
        }
    }

    @Override
    public SortedMap<String, String> getVariantProperties() {
        return EMPTY_SORTED_MAP;
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

    @Override
    public RecordId getMaster() {
        return this;
    }

    @Override
    public boolean isMaster() {
        return true;
    }
}
