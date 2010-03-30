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

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.RecordId;
import org.lilycms.util.ArgumentValidator;


/**
 * Utility class to generate and decode record id's.
 * 
 */
public class IdGeneratorImpl implements IdGenerator {
    
    private static enum IdIdentifier{ USER((byte)0), UUID((byte)1), VARIANT((byte)2);
    
        private final byte identifierByte;

        IdIdentifier(byte identifierByte) {
            this.identifierByte = identifierByte;
        }
        
        public byte getIdentifierByte() {
            return identifierByte;
        }
    };
    
    private static Map<Byte, IdIdentifier> identifierByteMap = new HashMap<Byte, IdIdentifier>();
    static {
        for (IdIdentifier identifier : IdIdentifier.values()) {
            identifierByteMap.put(new Byte(identifier.getIdentifierByte()), identifier);
        }
        
    }
    
    /* (non-Javadoc)
     * @see org.lilycms.repository.impl.IdGenerator#newRecordId()
     */
    public RecordId newRecordId() {
        //TODO configure the default recordId type
        return new UUIDRecordId(this);
    }
    
    /* (non-Javadoc)
     * @see org.lilycms.repository.impl.IdGenerator#newVariantRecordId(org.lilycms.repository.api.RecordId, java.util.Map)
     */
    public RecordId newRecordId(RecordId masterRecordId, Map<String, String> variantProperties) {
        ArgumentValidator.notNull(masterRecordId, "masterRecordId");
        ArgumentValidator.notNull(variantProperties, "variantProperties");
        return new VariantRecordId(masterRecordId, variantProperties, this);
    }
    
    /* (non-Javadoc)
     * @see org.lilycms.repository.impl.IdGenerator#newRecordId(java.lang.String)
     */
    public RecordId newRecordId(String userProvidedId) {
        ArgumentValidator.notNull(userProvidedId, "userProvidedId");
        return new UserRecordId(userProvidedId, this);
    }
    
    // Bytes
    // An identifier byte is put behind the bytes provided by the RecordId itself
    
    protected byte[] toBytes(UUIDRecordId uuidRecordId) {
        return toBytes(uuidRecordId.getBasicBytes(), IdIdentifier.UUID.getIdentifierByte());
    }
    
    protected byte[] toBytes(UserRecordId userRecordId) {
        return toBytes(userRecordId.getBasicBytes(), IdIdentifier.USER.getIdentifierByte());
    }
    
    protected byte[] toBytes(VariantRecordId variantRecordId) {
        return toBytes(variantRecordId.getBasicBytes(), IdIdentifier.VARIANT.getIdentifierByte());
    }
    

    private byte[] toBytes(byte[] basicBytes, byte identifier) {
        byte[] bytes = new byte[basicBytes.length+1];
        Bytes.putBytes(bytes, 0, basicBytes, 0, basicBytes.length);
        bytes[basicBytes.length] = identifier;
        return bytes;
    }
    
    /* (non-Javadoc)
     * @see org.lilycms.repository.impl.IdGenerator#fromBytes(byte[])
     */
    public RecordId fromBytes(byte[] bytes) {
        byte identifierByte = bytes[bytes.length-1];
        IdIdentifier idIdentifier = identifierByteMap.get(new Byte(identifierByte));
        RecordId recordId = null;
        switch (idIdentifier) {
        case UUID:
            recordId = new UUIDRecordId(Bytes.toLong(bytes, 0, 8), Bytes.toLong(bytes, 8, 8), this);
            break;

        case USER:
            recordId = new UserRecordId(Bytes.head(bytes, bytes.length-1), this);
            break;
            
        case VARIANT:
            recordId = new VariantRecordId(Bytes.head(bytes, bytes.length-1), this);
            break;
            
        default:
            break;
        }
        return recordId;
    }
    
    // Strings
    // The prefix string (e.g. "UUID:") is put before the string provided by the recordId itself
    
    protected String toString(UUIDRecordId uuidRecordId) {
        return IdIdentifier.UUID.name() + ":" + uuidRecordId.getBasicString();
    }
    
    protected String toString(UserRecordId userRecordId) {
        return IdIdentifier.USER.name() + ":" + userRecordId.getBasicString();
    }
    
    // The variantproperties are appended to the string of the master record and are also prefixed
    protected String toString(VariantRecordId variantRecordId) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(variantRecordId.getMaster().toString());
        stringBuilder.append(IdIdentifier.VARIANT.name());
        stringBuilder.append(":");
        stringBuilder.append(variantRecordId.getVariantPropertiesString());
        return stringBuilder.toString();
    }
    
    /* (non-Javadoc)
     * @see org.lilycms.repository.impl.IdGenerator#fromString(java.lang.String)
     */
    public RecordId fromString(String recordIdString) {
        int indexOfVariant = recordIdString.indexOf(IdIdentifier.VARIANT.name()+":", 0);
        if (indexOfVariant != -1) {
            String variantPropertiesString = recordIdString.substring(indexOfVariant+(IdIdentifier.VARIANT.name()+":").length());
            RecordId masterRecordId = fromString(recordIdString.substring(0, indexOfVariant));
            return new VariantRecordId(masterRecordId, variantPropertiesString, this);
        } else {
            if (recordIdString.startsWith(IdIdentifier.UUID.name() + ":")) {
                return new UUIDRecordId(recordIdString.substring((IdIdentifier.UUID.name() + ":").length()), this);
            }
            if (recordIdString.startsWith(IdIdentifier.USER.name()+":")) {
                return new UserRecordId(recordIdString.substring((IdIdentifier.USER.name()+":").length()), this);
            }
        }
        Byte identifierByte = Byte.valueOf(recordIdString.substring(recordIdString.length()-1, recordIdString.length()));
        IdIdentifier idVersion = identifierByteMap.get(identifierByte);
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
    
}
