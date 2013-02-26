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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class FieldFlagsTest {
    @Test
    public void testExistsFlag() {
        byte flags;

        flags = FieldFlags.get(true);
        assertEquals(0, flags);
        assertTrue(FieldFlags.exists(flags));
        assertFalse(FieldFlags.isDeletedField(flags));

        flags = FieldFlags.get(false);
        assertEquals(1, flags);
        assertFalse(FieldFlags.exists(flags));
        assertTrue(FieldFlags.isDeletedField(flags));
    }

    @Test
    public void testMetadataVersion() {
        byte flags;

        flags = FieldFlags.get(true, 1);
        assertEquals(0x02, flags);
        assertEquals(1, FieldFlags.getFieldMetadataVersion(flags));

        for (int i = 0; i < 8; i++) {
            flags = FieldFlags.get(true, i);
            assertEquals(i, FieldFlags.getFieldMetadataVersion(flags));
        }
    }
}
