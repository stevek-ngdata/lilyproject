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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.RecordId;

public class VariantRecordId implements RecordId {

    private final RecordId masterRecordId;
    private final SortedMap<String, String> variantProperties;
    private final IdGeneratorImpl idGenerator;

    private byte[] recordIdBytes;
    private String recordIdString;

    protected VariantRecordId(RecordId masterRecordId, Map<String, String> variantProperties,
                              IdGeneratorImpl idGenerator) {
        this.masterRecordId = masterRecordId;

        SortedMap<String, String> varProps = createVariantPropertiesMap();
        varProps.putAll(variantProperties);
        this.variantProperties = Collections.unmodifiableSortedMap(varProps);

        this.idGenerator = idGenerator;
    }

    protected VariantRecordId(RecordId masterRecordId, DataInput dataInput, IdGeneratorImpl idGenerator) {
        this.masterRecordId = masterRecordId;
        this.idGenerator = idGenerator;

        SortedMap<String, String> varProps = createVariantPropertiesMap();
        while (dataInput.getPosition() < dataInput.getSize()) {
            String dimension = dataInput.readVUTF();
            String dimensionValue = dataInput.readVUTF();

            IdGeneratorImpl.checkVariantPropertyNameValue(dimension);
            IdGeneratorImpl.checkVariantPropertyNameValue(dimensionValue);
            varProps.put(dimension, dimensionValue);
        }
        this.variantProperties = Collections.unmodifiableSortedMap(varProps);
    }

    private SortedMap<String, String> createVariantPropertiesMap() {
        // Make sure they are always sorted the same way
        return new TreeMap<String, String>();
    }

    public String toString() {
        if (recordIdString == null) {
            recordIdString = idGenerator.toString(this);
        }
        return recordIdString;
    }

    @Override
    public byte[] toBytes() {
        if (recordIdBytes == null) {
            DataOutput dataOutput = new DataOutputImpl();
            writeBytes(dataOutput);
            recordIdBytes = dataOutput.toByteArray();
        }
        return recordIdBytes;
    }

    @Override
    public final void writeBytes(DataOutput dataOutput) {
        if (recordIdBytes == null) {
            masterRecordId.writeBytes(dataOutput);

            // TODO this needs to be designed some other way
            if (masterRecordId instanceof UserRecordId) {
                dataOutput.writeByte((byte) 0);
            }

            Set<Entry<String, String>> entrySet = variantProperties.entrySet();
            for (Entry<String, String> entry : entrySet) {
                // entry consists of the dimension and the dimension value
                dataOutput.writeVUTF(entry.getKey());
                dataOutput.writeVUTF(entry.getValue());
            }

        } else {
            dataOutput.writeBytes(recordIdBytes);
        }
    }

    @Override
    public RecordId getMaster() {
        return masterRecordId;
    }

    @Override
    public boolean isMaster() {
        return false;
    }

    @Override
    public SortedMap<String, String> getVariantProperties() {
        return variantProperties;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((masterRecordId == null) ? 0 : masterRecordId.hashCode());
        result = prime * result + ((variantProperties == null) ? 0 : variantProperties.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        VariantRecordId other = (VariantRecordId) obj;
        if (masterRecordId == null) {
            if (other.masterRecordId != null) {
                return false;
            }
        } else if (!masterRecordId.equals(other.masterRecordId)) {
            return false;
        }
        if (variantProperties == null) {
            if (other.variantProperties != null) {
                return false;
            }
        } else if (!variantProperties.equals(other.variantProperties)) {
            return false;
        }
        return true;
    }
}
