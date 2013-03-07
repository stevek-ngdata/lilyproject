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
package org.lilyproject.repository.impl;

/**
 * Helper code to deal with the prefix-byte that is stored before each serialized field value.
 *
 * <p>This prefix byte contains a number of flags or other metadata, currently these are,
 * counting from the rightmost bit:</p>
 *
 * <ul>
 *     <li>bit 1: exists flag: does the field exist or not? 0=exists, 1=deleted</li>
 *     <li>bit 2-3-4: field meta version: is 000 if there is no field metadata appended to the field,
 *     otherwise it contains the version number of the encoding style of the metadata.</li>
 *     <li>bit 5-6-7-8: available for future uses</li>
 * </ul>
 */
public class FieldFlags {
    /**
     * Number of bytes taken up by the field flags. Since it is just one byte, this is 1, but it's useful
     * to have to constant for semantic reasons.
     */
    public static final int SIZE_OF_FIELD_FLAGS = 1;

    public static final byte DEFAULT = 0;

    /**
     * Flag to indicate deleted field, default is existing field.
     */
    public static final byte DELETED = 1; // 00 00 00 01

    /**
     * Flag to indicate metadata presence, default is no metadata.
     */
    public static final byte METADATA_V1 = 2; // 00 00 00 10

    private static final byte[] DELETE_MARKER = new byte[] { DELETED };

    private FieldFlags() {
    }


    public static final byte[] getDeleteMarker() {
        return DELETE_MARKER;
    }

    public static final boolean isDeletedField(byte flags) {
        return (flags & DELETED) == 1;
    }

    public static final boolean exists(byte flags) {
        return (flags & DELETED) == 0;
    }

    public static final int getFieldMetadataVersion(byte flags) {
        return (flags & 0x0E /* 00 00 11 10 */) >> 1;
    }
}
