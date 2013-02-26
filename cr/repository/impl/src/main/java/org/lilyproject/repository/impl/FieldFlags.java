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

import org.lilyproject.util.hbase.LilyHBaseSchema;

/**
 * Helper code to deal with the prefix-byte that is stored before each serialized field.
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
    public static final int SIZE = 1;
    public static final int NO_METADATA = 0;

    public static byte get(boolean exists) {
        if (exists) {
            return LilyHBaseSchema.EXISTS_FLAG;
        } else {
            return LilyHBaseSchema.DELETE_FLAG;
        }
    }

    public static byte[] getAsArray(boolean exists) {
        if (exists) {
            return new byte[] {LilyHBaseSchema.EXISTS_FLAG};
        } else {
            return new byte[] {LilyHBaseSchema.DELETE_FLAG};
        }
    }

    public static byte get(boolean exists, int fieldMetadataVersion) {
        byte result = 0;
        if (!exists) {
            result = (byte)(result | LilyHBaseSchema.DELETE_FLAG);
        }

        if (fieldMetadataVersion != 0) {
            if (fieldMetadataVersion < 0 || fieldMetadataVersion >= 8) {
                throw new IllegalArgumentException("Field metadata version out of range: " + fieldMetadataVersion);
            }
            result = (byte)(result | (fieldMetadataVersion << 1));
        }

        return result;
    }

    public static boolean isDeletedField(byte flags) {
        return (flags & 1) == 1;
    }

    public static boolean exists(byte flags) {
        return (flags & 1) == 0;
    }

    public static int getFieldMetadataVersion(byte flags) {
        return (flags & 0x0E) >> 1;
    }
}
