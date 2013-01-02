/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.impl.compat;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.impl.id.UUIDRecordId;
import org.lilyproject.repository.impl.id.UserRecordId;
import org.lilyproject.repository.impl.id.VariantRecordId;

/**
 * This class contains code to parse Lily <= 1.1 style record id's encoded as bytes.
 */
public class Lily11RecordIdDecoder {
    private static Constructor UUID_CONSTRUCTOR;

    static {
        try {
            UUID_CONSTRUCTOR = UUIDRecordId.class.getDeclaredConstructor(UUID.class, IdGeneratorImpl.class);
            UUID_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Constructor USER_CONSTRUCTOR;

    static {
        try {
            USER_CONSTRUCTOR = UserRecordId.class.getDeclaredConstructor(String.class, IdGeneratorImpl.class);
            USER_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Constructor VARIANT_CONSTRUCTOR;

    static {
        try {
            VARIANT_CONSTRUCTOR = VariantRecordId.class.getDeclaredConstructor(RecordId.class, Map.class, IdGeneratorImpl.class);
            VARIANT_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private IdGeneratorImpl idGenerator;

    public Lily11RecordIdDecoder(IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
    }

    private static enum IdIdentifier {
        USER((byte)0), UUID((byte)1), VARIANT((byte)2);

        private final byte identifierByte;

        IdIdentifier(byte identifierByte) {
            this.identifierByte = identifierByte;
        }

        public byte getIdentifierByte() {
            return identifierByte;
        }
    }

    private static IdIdentifier[] IDENTIFIERS;

    static {
        IDENTIFIERS = new IdIdentifier[3];
        IDENTIFIERS[0] = IdIdentifier.USER;
        IDENTIFIERS[1] = IdIdentifier.UUID;
        IDENTIFIERS[2] = IdIdentifier.VARIANT;
    }

    public static RecordId decode(DataInput dataInput, IdGenerator idGenerator) {
        try {
            // Read the identifier byte at the end of the input
            int pos = dataInput.getPosition();
            int size = dataInput.getSize();
            dataInput.setPosition(size - 1);
            byte identifierByte = dataInput.readByte();
            dataInput.setPosition(pos);

            // Virtually remove the identifier byte from the input
            dataInput.setSize(size - 1);

            IdIdentifier idIdentifier = IDENTIFIERS[identifierByte];
            RecordId recordId = null;
            switch (idIdentifier) {
                case UUID:
                    UUID uuid = new UUID(dataInput.readLong(), dataInput.readLong());
                    recordId = (RecordId)UUID_CONSTRUCTOR.newInstance(uuid, idGenerator);
                    break;
                case USER:
                    String basicRecordIdString = dataInput.readUTF();
                    checkIdString(basicRecordIdString, "record id");
                    recordId = (RecordId)USER_CONSTRUCTOR.newInstance(basicRecordIdString, idGenerator);
                    break;
                case VARIANT:
                    int position = dataInput.getPosition();
                    int length = dataInput.getSize();
                    dataInput.setPosition(length - 8);
                    int nrOfVariants = dataInput.readInt();
                    int masterRecordIdLength = dataInput.readInt();

                    DataInput masterRecordIdInput = new DataInputImpl((DataInputImpl)dataInput, position, masterRecordIdLength);
                    RecordId masterRecordId = decode(masterRecordIdInput, idGenerator);
                    dataInput.setPosition(masterRecordIdLength);

                    SortedMap<String, String> varProps = new TreeMap<String, String>();
                    for (int i = 0; i < nrOfVariants; i++) {
                        String dimension = dataInput.readUTF();
                        String dimensionValue = dataInput.readUTF();

                        checkVariantPropertyNameValue(dimension);
                        checkVariantPropertyNameValue(dimensionValue);
                        varProps.put(dimension, dimensionValue);
                    }

                    recordId = (RecordId)VARIANT_CONSTRUCTOR.newInstance(masterRecordId, varProps, idGenerator);
                    break;
                default:
                    // will already have failed on the IDENTIFIERS array lookup above
                    break;
            }
            return recordId;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    public static Link decodeLink(DataInput dataInput, IdGenerator idGenerator) {
        // Format: see toBytes.

        int recordIdLength = dataInput.readInt();
        byte[] recordIdBytes = null;
        if (recordIdLength > 0) {
            recordIdBytes = dataInput.readBytes(recordIdLength);
        }
        String args = dataInput.readUTF();

        if (recordIdLength == 0 && args == null) {
            return new Link();
        }

        Link.LinkBuilder builder = Link.newBuilder();
        if (recordIdLength > 0) {
            RecordId id = decode(new DataInputImpl(recordIdBytes), idGenerator);
            builder.recordId(id);
        }

        if (args != null && args.length() > 0) {
            argsFromString(args, builder, args /* does not matter, should never be invalid */);
        }

        return builder.create();
    }

    private static void argsFromString(String args, Link.LinkBuilder builder, String link) {
        String[] variantStringParts = args.split(",");
        for (String part : variantStringParts) {
            int eqPos = part.indexOf('=');
            if (eqPos == -1) {
                String thing = part.trim();
                if (thing.equals("*")) {
                    // this is the default, but if users want to make explicit, allow them
                    builder.copyAll(true);
                } else if (thing.equals("!*")) {
                    builder.copyAll(false);
                } else if (thing.startsWith("+") && thing.length() > 1) {
                    builder.copy(thing.substring(1));
                } else if (thing.startsWith("-") && thing.length() > 1) {
                    builder.remove(thing.substring(1));
                } else {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
            } else {
                String name = part.substring(0, eqPos).trim();
                String value = part.substring(eqPos + 1).trim();
                if (name.length() == 0 || value.length() == 0) {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
                builder.set(name, value);
            }
        }
    }
}
