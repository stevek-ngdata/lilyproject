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
package org.lilyproject.repository.impl.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;

import static org.junit.Assert.*;

public class IdGeneratorImplTest {

    @Test
    public void testRandomUUID() {
        UUID newUUid = UUID.randomUUID();
        String uuidString = newUUid.toString();
        UUID uuidFromString = UUID.fromString(uuidString);
        assertEquals(uuidFromString.toString(), uuidString);
    }

    @Test
    public void testIdGeneratorDefault() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.newRecordId();
        assertEquals(recordId, idGenerator.fromBytes(recordId.toBytes()));
        assertEquals(recordId, idGenerator.fromString(recordId.toString()));
    }

    @Test
    public void testUUID() {
        IdGenerator idGenerator = new IdGeneratorImpl();

        // Test string representation
        String uuidRecordIDString = "UUID.d27cdb6e-ae6d-11cf-96b8-444553540000";
        assertEquals(uuidRecordIDString, idGenerator.fromString(uuidRecordIDString).toString());

        // Check it's not recognized as a variant
        assertTrue(idGenerator.fromString(uuidRecordIDString).isMaster());

        // Test bytes representation
        byte[] uuidRecordIdBytes = new byte[] {1, -46, 124, -37, 110, -82, 109, 17, -49, -106, -72, 68, 69, 83, 84, 0, 0};
        assertArrayEquals(uuidRecordIdBytes, idGenerator.fromBytes(uuidRecordIdBytes).toBytes());

        assertEquals(uuidRecordIDString, idGenerator.fromBytes(uuidRecordIdBytes).toString());

    }

    @Test
    public void testUSER() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId newRecordId = idGenerator.newRecordId("aUserId");
        String userRecordIDString = "USER.aUserId";

        // Check it's not recognized as a variant
        assertTrue(newRecordId.isMaster());

        // Test string representation
        assertEquals(newRecordId, idGenerator.fromString(userRecordIDString));
        assertEquals(userRecordIDString, idGenerator.fromString(userRecordIDString).toString());

        // Test bytes representation cycle
        byte[] userRecordIdBytes = newRecordId.toBytes();
        assertArrayEquals(userRecordIdBytes, idGenerator.fromBytes(userRecordIdBytes).toBytes());

        assertEquals(userRecordIDString, idGenerator.fromBytes(userRecordIdBytes).toString());

        // Test the bytes representation is really what we expect it to be
        byte[] idBytes = new byte[] {0, 65, 66, 67};
        String idString = "USER.ABC";
        assertArrayEquals(idBytes, idGenerator.fromString(idString).toBytes());


        byte[] withDotsBytes = new byte[] { 0, 65, 66, ':', 67, 68, '.', 69, 70 };
        String withDots = "USER.AB:CD\\.EF";
        assertArrayEquals(withDotsBytes, idGenerator.fromString(withDots).toBytes());
    }

    @Test
    public void testUUIDWithVariantSingleProperty() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId masterRecordId = idGenerator.newRecordId();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dim1", "dimvalue1");
        RecordId variantRecordId = idGenerator.newRecordId(masterRecordId, variantProperties);

        // Test it is recognized as variant
        assertFalse(variantRecordId.isMaster());

        // Test string representation is what it is supposed to be
        String variantRecordIdString = masterRecordId.toString() + ".dim1=dimvalue1";
        assertEquals(variantRecordIdString, variantRecordId.toString());
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordIdString));

        // Test round-trip string & bytes conversion
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordId.toString()));
        assertEquals(variantRecordId, idGenerator.fromBytes(variantRecordId.toBytes()));

        // Test bytes representation is really what we expect it to be
        byte[] masterIdBytes = new byte[] {
                /* uuid type marker */1,
                /* uuid bytes */-46, 124, -37, 110, -82, 109, 17, -49, -106, -72, 68, 69, 83, 84, 0, 0 };

        byte[] variantIdBytes = new byte[] {
                /* uuid type marker */1,
                /* uuid bytes */-46, 124, -37, 110, -82, 109, 17, -49, -106, -72, 68, 69, 83, 84, 0, 0
                /* length of key (vint) */, 1
                /* the key (letter X) */, 88
                /* length of value (vint) */, 3
                /* the value (ABC) */, 65, 66, 67 };

        RecordId variantId = idGenerator.newRecordId(idGenerator.fromBytes(masterIdBytes),
                Collections.singletonMap("X", "ABC"));
        assertArrayEquals(variantIdBytes, variantId.toBytes());
    }

    @Test
    public void testUUUIDWithMultipleProperties() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId masterRecordId = idGenerator.newRecordId();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dim1", "dimvalue1");
        variantProperties.put("dim2", "dimvalue2");

        RecordId variantRecordId = idGenerator.newRecordId(masterRecordId, variantProperties);

        // Test string representation is what it is supposed to be
        String variantRecordIdString = masterRecordId.toString() + ".dim1=dimvalue1,dim2=dimvalue2";
        assertEquals(variantRecordIdString, variantRecordId.toString());
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordIdString));

        // Test round-trip string & bytes conversion
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordIdString));
        assertEquals(variantRecordId, idGenerator.fromBytes(variantRecordId.toBytes()));
    }

    @Test
    public void testUserIdWithVariantProperties() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId masterId = idGenerator.newRecordId("marvellous");
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("a", "x");
        variantProperties.put("aa", "xx");

        RecordId variantId = idGenerator.newRecordId(masterId, variantProperties);

        // Test it is recognized as variant
        assertFalse(variantId.isMaster());

        // Test round-trip string & bytes conversion
        assertEquals(variantId, idGenerator.fromBytes(variantId.toBytes()));
        assertEquals(variantId, idGenerator.fromString(variantId.toString()));

        // Test string representation is what it is supposed to be
        String expectedString = "USER.marvellous.a=x,aa=xx";
        assertEquals(expectedString, variantId.toString());

        // Test bytes representation is what it is supposed to be
        // Note that the keys should always be in sorted order
        byte[] variantIdBytes = new byte[] {
                /* user type marker */0,
                /* 'marvellous' as bytes */109, 97, 114, 118, 101, 108, 108, 111, 117, 115,
                /* separator byte between id and the props */0,
                /* -- first property -- */
                /* length of key */1,
                /* the key (a) */97,
                /* length of value */1,
                /* the value (x) */120,
                /* -- second property -- */
                /* length of key */2,
                /* the key (aa) */97, 97,
                /* length of value */2,
                /* the value (xx) */120, 120
        };
        assertArrayEquals(variantIdBytes, variantId.toBytes());
    }

    @Test
    public void testNullCharacterNotAllowedInUserId() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        try {
            idGenerator.fromString("USER.hello\u0000world");
            fail("Expected an exception when using zero byte in string.");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            idGenerator.newRecordId("hello\u0000world");
            fail("Expected an exception when using zero byte in string.");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testReservedCharacterEscapingInUserId() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        char[] chars = new char[] {'.', ',', '=' };
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            RecordId recordId = idGenerator.newRecordId("hello" + c + "world");

            String idString = recordId.toString();
            assertEquals("USER.hello\\" + c + "world", idString);
        }
    }

    @Test
    public void testReservedCharacterUnEscapingInUserId() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        String idString = "USER.special\\.characters\\,are\\=fun\\\\.key=hoeba\\=hoep";
        RecordId recordId = idGenerator.fromString(idString);
        String encodedString = recordId.toString();

        assertEquals(idString, encodedString);
    }

    @Test
    public void testReservedCharsInUserIdFromString() {
        char[] chars = new char[] {'.', ',', '=' };
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            testNotAllowedChar(c);
            testEscapedNotAllowedChar(c);
        }
    }

    private void testNotAllowedChar(char c) {
        IdGenerator idGenerator = new IdGeneratorImpl();
        try {
            idGenerator.fromString("USER.hello" + c + "world");
            fail("Expected an exception when using character " + c);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void testEscapedNotAllowedChar(char c) {
        IdGenerator idGenerator = new IdGeneratorImpl();
        String idString = "USER.hello\\" + c + "world";
        try {
            RecordId recordId = idGenerator.fromString(idString);
            assertEquals(idString, recordId.toString());
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}
