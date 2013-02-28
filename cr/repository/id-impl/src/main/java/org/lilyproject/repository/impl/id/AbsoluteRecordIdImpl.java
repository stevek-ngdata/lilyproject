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
package org.lilyproject.repository.impl.id;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.lilyproject.repository.api.AbsoluteRecordId;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.util.ArgumentValidator;

public class AbsoluteRecordIdImpl implements AbsoluteRecordId {

    private final String table;
    private final RecordId recordId;

    public AbsoluteRecordIdImpl(String table, RecordId recordId) {
        ArgumentValidator.notNull(table, "table");
        ArgumentValidator.notNull(recordId, "recordId");
        this.table = table;
        this.recordId = recordId;
    }

    @Override
    public RecordId getRecordId() {
        return recordId;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        byte[] tableBytes = table.getBytes();
        byte[] recordIdBytes = recordId.toBytes();
        try {
            dataOutput.writeInt(tableBytes.length);
            dataOutput.write(tableBytes);
            dataOutput.writeInt(recordIdBytes.length);
            dataOutput.write(recordIdBytes);
        } catch (IOException ioe) {
            throw new RuntimeException("Error serializing AbsoluteRecordId: " + toString(), ioe);
        }
        return byteArrayOutputStream.toByteArray();
    }
    
    public static AbsoluteRecordId fromBytes(byte[] bytes, IdGenerator idGenerator) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInput dataInput = new DataInputStream(byteArrayInputStream);
        
        byte[] tableBytes;
        byte[] recordIdBytes;
        
        try {
            tableBytes = new byte[dataInput.readInt()];
            dataInput.readFully(tableBytes, 0, tableBytes.length);
            recordIdBytes = new byte[dataInput.readInt()];
            dataInput.readFully(recordIdBytes, 0, recordIdBytes.length);
        } catch (IOException ioe) {
            throw new RuntimeException("Error while deserializing AbsoluteRecordId", ioe);
        }
        
        return new AbsoluteRecordIdImpl(new String(tableBytes), idGenerator.fromBytes(recordIdBytes));
        
    }
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
    

}
