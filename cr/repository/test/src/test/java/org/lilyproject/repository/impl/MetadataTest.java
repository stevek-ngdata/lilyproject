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
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.MetadataBuilder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MetadataTest {
    /**
     * Tests that all supported types survive a serialize-deserialize cycle.
     */
    @Test
    public void testSerDeser() {
        MetadataBuilder metadataBuilder = new MetadataBuilder();
        metadataBuilder.value("string", "value");
        metadataBuilder.value("int", 5);
        metadataBuilder.value("long", 99999999999L);
        metadataBuilder.value("float", 3.33f);
        metadataBuilder.value("double", 6.66d);
        metadataBuilder.value("boolean", Boolean.TRUE);
        metadataBuilder.value("bytes", new ByteArray("foobar".getBytes()));

        DataOutput output = new DataOutputImpl();
        MetadataSerDeser.write(metadataBuilder.build(), output);
        byte[] metadataBytes = output.toByteArray();

        Metadata readMetadata = MetadataSerDeser.read(new DataInputImpl(metadataBytes));
        assertEquals("value", readMetadata.get("string"));
        assertEquals(5, (int)readMetadata.getInt("int", null));
        assertEquals(99999999999L, (long)readMetadata.getLong("long", null));
        assertEquals(3.33f, readMetadata.getFloat("float", null), 0.001f);
        assertEquals(6.66d, (double)readMetadata.getDouble("double", null), 0.001d);
        assertEquals(true, readMetadata.getBoolean("boolean", null));
        assertArrayEquals("foobar".getBytes(), readMetadata.getBytes("bytes").getBytes());
    }
}
