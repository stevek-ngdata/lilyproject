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
package org.lilyproject.repository.impl.recordid;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.util.ArgumentValidator;


public class IdGeneratorImpl implements IdGenerator {
    
    protected static enum IdType {
        USER((byte)0, new UserRecordIdFactory()),
        UUID((byte)1, new UUIDRecordIdFactory());
    
        private final byte identifierByte;
        private final RecordIdFactory factory;

        IdType(byte identifierByte, RecordIdFactory factory) {
            this.identifierByte = identifierByte;
            this.factory = factory;
        }
        
        public byte getIdentifierByte() {
            return identifierByte;
        }

        public RecordIdFactory getFactory() {
            return factory;
        }
    }

    private static IdType[] ID_TYPES = IdType.values();

    @Override
    public RecordId newRecordId() {
        return new UUIDRecordId(this);
    }

    @Override
    public RecordId newRecordId(RecordId masterRecordId, Map<String, String> variantProperties) {
        ArgumentValidator.notNull(masterRecordId, "masterRecordId");
        ArgumentValidator.notNull(variantProperties, "variantProperties");

        if (!masterRecordId.isMaster())
            throw new IllegalArgumentException("Specified masterRecordId is a variant record ID.");

        if (variantProperties.isEmpty())
            return masterRecordId;

        checkReservedCharacters(variantProperties);
        
        return new VariantRecordId(masterRecordId, variantProperties, this);
    }

    @Override
    public RecordId newRecordId(Map<String, String> variantProperties) {
        return newRecordId(newRecordId(), variantProperties);
    }

    @Override
    public RecordId newRecordId(String userProvidedId) {
        ArgumentValidator.notNull(userProvidedId, "userProvidedId");
        checkIdString(userProvidedId, "record id");
        return new UserRecordId(userProvidedId, this);
    }
    
    @Override
    public RecordId newRecordId(String userProvidedId, Map<String, String> variantProperties) {
        return newRecordId(newRecordId(userProvidedId), variantProperties);
    }

    @Override
    public RecordId fromBytes(byte[] bytes) {
        return fromBytes(new DataInputImpl(bytes));
    }

    @Override
    public RecordId fromBytes(DataInput dataInput) {
        byte idType = dataInput.readByte();
        // Note: will throw arrayindexoutofbounds if id is not known
        IdType id = ID_TYPES[idType];

        DataInput[] splitted = id.factory.splitInMasterAndVariant(dataInput);
        DataInput masterIdInput = splitted[0];
        DataInput variantParamsInput = splitted[1];

        RecordId masterRecordId = id.factory.fromBytes(masterIdInput, this);

        if (variantParamsInput != null) {
            return new VariantRecordId(masterRecordId, variantParamsInput, this);
        } else {
            return masterRecordId;
        }
    }
    
    // Strings
    // The prefix string (e.g. "UUID.") is put before the string provided by the recordId itself
    
    protected String toString(UUIDRecordId uuidRecordId) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(IdType.UUID.name());
        stringBuilder.append(".");
        stringBuilder.append(uuidRecordId.getBasicString());
        return stringBuilder.toString();
    }
    
    protected String toString(UserRecordId userRecordId) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(IdType.USER.name());
        stringBuilder.append(".");
        stringBuilder.append(userRecordId.getBasicString());
        return stringBuilder.toString();
    }
    
    // The variantproperties are appended to the string of the master record
    protected String toString(VariantRecordId variantRecordId) {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(variantRecordId.getMaster().toString());
        stringBuilder.append(".");

        boolean first = true;
        for (Map.Entry<String, String> entry : variantRecordId.getVariantProperties().entrySet()) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(",");
            }
            
            stringBuilder.append(entry.getKey()).append('=').append(entry.getValue());
        }

        return stringBuilder.toString();
    }
    
    @Override
    public RecordId fromString(String recordIdString) {
        int firstDotPos = recordIdString.indexOf('.');

        if (firstDotPos == -1) {
            throw new IllegalArgumentException("Invalid record id, contains no dot: " + recordIdString);
        }

        String type = recordIdString.substring(0, firstDotPos).trim();
        String id;
        String variantString = null;

        int secondDotPos = recordIdString.indexOf('.', firstDotPos + 1);

        if (secondDotPos == -1) {
            id = recordIdString.substring(firstDotPos + 1).trim();
        } else {
            id = recordIdString.substring(firstDotPos + 1, secondDotPos).trim();
            variantString = recordIdString.substring(secondDotPos + 1);
        }

        RecordIdFactory factory = null;
        for (IdType idType : ID_TYPES) {
            if (type.equals(idType.name())) {
                factory = idType.factory;
                break;
            }
        }
        
        if (factory == null) {
            throw new IllegalArgumentException("Invalid record id: unknown type '" + type + "' in record id '" +
                    recordIdString + "'.");
        }

        RecordId masterRecordId = factory.fromString(id, this);

        if (variantString == null) {
            return masterRecordId;
        }

        // Parse the variant string
        Map<String, String> variantProps = new HashMap<String, String>();

        String[] variantStringParts = variantString.split(",");
        for (String part : variantStringParts) {
            int eqPos = part.indexOf('=');
            if (eqPos == -1) {
                throw new IllegalArgumentException("Invalid record id: " + recordIdString);
            }

            String name = part.substring(0, eqPos).trim();
            String value = part.substring(eqPos + 1).trim();

            variantProps.put(name, value);
        }

        return new VariantRecordId(masterRecordId, variantProps, this);
    }

    protected static void checkReservedCharacters(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            checkVariantPropertyNameValue(entry.getKey());
            checkVariantPropertyNameValue(entry.getValue());
        }
    }

    protected static void checkVariantPropertyNameValue(String text) {
        checkIdString(text, "variant property name or value");
    }

    protected static void checkIdString(String text, String what) {
        if (text.length() == 0) {
            throw new IllegalArgumentException("Zero-length " + what + " is not allowed.");
        }

        if (Character.isWhitespace(text.charAt(0)) || Character.isWhitespace(text.charAt(text.length() - 1))) {
            throw new IllegalArgumentException(what + " should not start or end with whitespace: \"" + text + "\".");
        }

        checkReservedCharacters(text);
    }

    protected static void checkReservedCharacters(String text) {
        for (int i = 0; i < text.length(); i++) {
            switch (text.charAt(i)) {
                case '.':
                case '=':
                case ',':
                    throw new IllegalArgumentException("Reserved record id character (one of: . , =) in \"" +
                            text + "\".");
            }
        }
    }
    
    @Override
    public SchemaId getSchemaId(byte[] id) {
        return new SchemaIdImpl(id);
    }

    @Override
    public SchemaId getSchemaId(String id) {
        return new SchemaIdImpl(id);
    }
    
    @Override
    public SchemaId getSchemaId(UUID id) {
        return new SchemaIdImpl(id);
    }

}
