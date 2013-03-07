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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.lilyproject.repository.api.AbsoluteRecordId;

import org.apache.commons.lang.StringEscapeUtils;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ArgumentValidator;

public class IdGeneratorImpl implements IdGenerator {

    protected static enum IdType {
        USER((byte) 0, new UserRecordIdFactory()),
        UUID((byte) 1, new UUIDRecordIdFactory());

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
    public AbsoluteRecordId newAbsoluteRecordId(String tableName, RecordId recordId) {
        return new AbsoluteRecordIdImpl(tableName, recordId);
    }

    @Override
    public AbsoluteRecordId newAbsoluteRecordId(String tableName, String userProvided) {
        return newAbsoluteRecordId(tableName, newRecordId(userProvided));
    }

    @Override
    public RecordId fromBytes(byte[] bytes) {
        return fromBytes(new DataInputImpl(bytes));
    }

    @Override
    public AbsoluteRecordId absoluteFromBytes(byte[] bytes) {
        return AbsoluteRecordIdImpl.fromBytes(bytes, this);
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
    // The prefix string (e.g. "UUID.") is put before the string provided by the
    // recordId itself

    protected String toString(UUIDRecordId uuidRecordId) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(IdType.UUID.name());
        stringBuilder.append(".");
        stringBuilder.append(uuidRecordId.getBasicString());
        return stringBuilder.toString();
    }

    protected String toString(UserRecordId userRecordId) {
        String idString = userRecordId.getBasicString();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(IdType.USER.name());
        stringBuilder.append(".");
        stringBuilder.append(escapeReservedCharacters(idString));
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

            stringBuilder.append(escapeReservedCharacters(entry.getKey())).append('=')
                    .append(escapeReservedCharacters(entry.getValue()));
        }

        return stringBuilder.toString();
    }

    private static String[] escapedSplit(String s, char delimiter) {
        ArrayList<String> split = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean escaped = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (escaped) {
                escaped = false;
                sb.append(c);
            } else if (delimiter == c) {
                split.add(sb.toString());
                sb = new StringBuffer();
            } else if ('\\' == c) {
                escaped = true;
                sb.append(c);
            } else {
                sb.append(c);
            }
        }
        split.add(sb.toString());

        return split.toArray(new String[0]);
    }

    @Override
    public RecordId fromString(String recordIdString) {
        String[] idParts = escapedSplit(recordIdString, '.');

        if (idParts.length <= 1) {
            throw new IllegalArgumentException("Invalid record id, contains no dot: " + recordIdString);
        }

        String type = idParts[0].trim();
        String id = idParts[1].trim();
        String variantString = idParts.length > 2 ? idParts[2] : null;

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

        checkEscapedReservedCharacters(id);
        RecordId masterRecordId = factory.fromString(StringEscapeUtils.unescapeJava(id), this);

        if (variantString == null) {
            return masterRecordId;
        }

        // Parse the variant string
        Map<String, String> variantProps = new HashMap<String, String>();

        String[] variantStringParts = escapedSplit(variantString, ',');
        for (String part : variantStringParts) {
            String[] keyVal = escapedSplit(part, '=');
            if (keyVal.length != 2) {
                throw new IllegalArgumentException("Invalid record id: " + recordIdString);
            }

            String name = StringEscapeUtils.unescapeJava(keyVal[0].trim());
            String value = StringEscapeUtils.unescapeJava(keyVal[1].trim());

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
                case '\u0000':
                    throw new IllegalArgumentException("Null characters may not be used in a record ID");
            }
        }
    }

    protected static void checkEscapedReservedCharacters(String text) {
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if ('\u0000' == c) {
                throw new IllegalArgumentException("Null characters may not be used in a record ID");
            } else {
                if (escaped) {
                    escaped = false;
                } else {
                    switch (text.charAt(i)) {
                        case '.':
                        case '=':
                        case ',':
                            throw new IllegalArgumentException(
                                    "Reserved characters [.,=] must be escaped in a RecordID using '\\'. RecordID = "
                                            + text);
                        case '\\':
                            escaped = true;
                    }
                }
            }
        }
    }

    protected static String escapeReservedCharacters(String text) {
        return text.replaceAll("([.,=\\\\])", "\\\\$1");
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
