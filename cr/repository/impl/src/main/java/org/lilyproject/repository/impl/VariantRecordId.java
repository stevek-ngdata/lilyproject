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
import java.util.Map.Entry;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;

public class VariantRecordId implements RecordId {

    private final RecordId masterRecordId;
    private final SortedMap<String, String> variantProperties;
    private final IdGeneratorImpl idGenerator;

    private byte[] recordIdBytes;
    private String recordIdString;

    protected VariantRecordId(RecordId masterRecordId, Map<String, String> variantProperties, IdGeneratorImpl idGenerator) {
        this.masterRecordId = masterRecordId;

        SortedMap<String, String> varProps = createVariantPropertiesMap();
        varProps.putAll(variantProperties);
        this.variantProperties = Collections.unmodifiableSortedMap(varProps);

        this.idGenerator = idGenerator;
    }
    
    /**
     * The bytes of the masterRecordId are appended with the bytes of the variantProperties
     * The variantProperties bytes ends with an integer indicating the number of variantProperties
     */
    protected VariantRecordId(DataInput dataInput, IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
        int position = dataInput.getPosition();
        int length = dataInput.getSize();
        dataInput.setPosition(length - 8);
        int nrOfVariants = dataInput.readInt();
        int masterRecordIdLength = dataInput.readInt();
        
        this.masterRecordId = idGenerator.fromBytes(new DataInputImpl((DataInputImpl)dataInput, position, masterRecordIdLength));
        dataInput.setPosition(masterRecordIdLength);
        
        SortedMap<String, String> varProps = createVariantPropertiesMap();
        for (int i = 0; i < nrOfVariants; i++) {
            String dimension = dataInput.readUTF();
            String dimensionValue = dataInput.readUTF();
            
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
    
    public byte[] toBytes() {
        if (recordIdBytes == null) {
            recordIdBytes = idGenerator.toBytes(this); 
        }
        return recordIdBytes;
    }
    
    public void writeBytes(DataOutput dataOutput) {
        if (recordIdBytes == null) {
            idGenerator.writeBytes(this, dataOutput);
        } else {
            dataOutput.writeBytes(recordIdBytes);
        }
    }

    /**
     * Writes the byte representation of the variant record id to the DataOutput, without adding the identifying byte.
     * <p> Note: The master record id part will contain its identifying byte though.
     */
    public void writeBasicBytes(DataOutput dataOutput) {
        int start = dataOutput.getSize();
        masterRecordId.writeBytes(dataOutput); // Write the whole masterRecordId bytes, not just the basic bytes
        int masterRecordIdLength = dataOutput.getSize() - start;
        
        Set<Entry<String,String>> entrySet = variantProperties.entrySet();
        for (Entry<String, String> entry : entrySet) {
            // entry consists of the dimension and the dimension value
            dataOutput.writeUTF(entry.getKey());
            dataOutput.writeUTF(entry.getValue());
        }
        // Write the amount of variantProperties
        // This should be written at the end so that a scan over rowkeys has all variants in-order
        dataOutput.writeInt(entrySet.size());
        dataOutput.writeInt(masterRecordIdLength);
    }
    
    public RecordId getMaster() {
        return masterRecordId;
    }

    public boolean isMaster() {
        return false;
    }

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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VariantRecordId other = (VariantRecordId) obj;
        if (masterRecordId == null) {
            if (other.masterRecordId != null)
                return false;
        } else if (!masterRecordId.equals(other.masterRecordId))
            return false;
        if (variantProperties == null) {
            if (other.variantProperties != null)
                return false;
        } else if (!variantProperties.equals(other.variantProperties))
            return false;
        return true;
    }
}
