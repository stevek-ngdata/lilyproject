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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.RecordId;
import org.lilycms.util.ArgumentValidator;


/**
 * Utility class to generate and decode record id's.
 * 
 */
public class IdGenerator {
    
    private static enum IdVersion{ USER((byte)0), UUID((byte)1);
    
        private final byte identifierByte;

        IdVersion(byte identifierByte) {
            this.identifierByte = identifierByte;
        }
        
        public byte getIdentifierByte() {
            return identifierByte;
        }
    };
    
    private static Map<Byte, IdVersion> byteMap = new HashMap<Byte, IdVersion>();
    static {
        for (IdVersion idVersion : IdVersion.values()) {
            byteMap.put(new Byte(idVersion.getIdentifierByte()), idVersion);
        }
    }
    
    public RecordId newRecordId() {
        //TODO configure the default recordId type
        return new UUIDRecordId(this);
    }
    
    public RecordId newRecordId(String userProvidedId) {
        ArgumentValidator.notNull(userProvidedId, "userProvidedId");
        return this.fromString(userProvidedId+"0");
    }
    
    public String toString(UUIDRecordId uuidRecordId) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(uuidRecordId.getUuid().toString());
        stringBuffer.append(IdVersion.UUID.getIdentifierByte());
        return stringBuffer.toString();
    }
    
    public String toString(UserRecordId userRecordId) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(userRecordId.recordIdString);
        stringBuffer.append(IdVersion.USER.getIdentifierByte());
        return stringBuffer.toString();
    }

    public byte[] toBytes(UUIDRecordId uuidRecordId) {
        UUID uuid = uuidRecordId.getUuid();
        byte[] bytes = new byte[17];
        Bytes.putLong(bytes, 0, uuid.getMostSignificantBits());
        Bytes.putLong(bytes, 8, uuid.getLeastSignificantBits());
        bytes[16] = IdVersion.UUID.getIdentifierByte();
        return bytes;
    }
    
    public byte[] toBytes(UserRecordId userRecordId) {
        int length = userRecordId.recordIdBytes.length;
        byte[] bytes = new byte[length+1];
        Bytes.putBytes(bytes, 0, userRecordId.recordIdBytes, 0, length);
        bytes[length] = IdVersion.USER.getIdentifierByte();
        return bytes;
    }

    public RecordId fromString(String recordIdString) {
        Byte identifierByte = Byte.valueOf(recordIdString.substring(recordIdString.length()-1, recordIdString.length()));
        IdVersion idVersion = byteMap.get(identifierByte);
        RecordId recordId = null;
        switch (idVersion) {
        case UUID:
            recordId = new UUIDRecordId(recordIdString.substring(0, recordIdString.length()-1), this);
            break;

        case USER:
            recordId = new UserRecordId(recordIdString.substring(0, recordIdString.length()-1), this);
            
        default:
            break;
        }
        return recordId;
    }

    public RecordId fromBytes(byte[] bytes) {
        byte identifierByte = bytes[bytes.length-1];
        IdVersion idVersion = byteMap.get(new Byte(identifierByte));
        RecordId recordId = null;
        switch (idVersion) {
        case UUID:
            recordId = new UUIDRecordId(Bytes.toLong(bytes, 0, 8), Bytes.toLong(bytes, 8, 8), this);
            break;

        case USER:
            recordId = new UserRecordId(Bytes.head(bytes, bytes.length-1), this);
            
        default:
            break;
        }
        return recordId;
    }
    
}
