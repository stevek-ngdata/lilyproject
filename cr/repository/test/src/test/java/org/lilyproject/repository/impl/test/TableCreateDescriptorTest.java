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
package org.lilyproject.repository.impl.test;

import org.junit.Test;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.util.hbase.TableConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TableCreateDescriptorTest {

    @Test
    public void testCreateInstance_OnlyName() {
        TableCreateDescriptor descriptor = new TableCreateDescriptor("mytable");
        assertEquals("mytable", descriptor.getName());
        assertNull(descriptor.getSplitKeys());
    }

    @Test
    public void testCreateInstanceWithSplits_NoPrefix() {
        byte[][] splitKeys = TableConfig.parseSplitKeys(null, "04,08", null);
        TableCreateDescriptor descriptor = new TableCreateDescriptor("mytable", splitKeys);
        assertEquals(2, descriptor.getSplitKeys().length);
    }

    @Test
    public void testCreateInstanceWithSplits_WithPrefix() {
        byte[][] inputSplitKeys = TableConfig.parseSplitKeys(null, "04,08,12", "\\x01");
        TableCreateDescriptor descriptor = new TableCreateDescriptor("mytable", inputSplitKeys);

        byte[][] splitKeys = descriptor.getSplitKeys();
        assertEquals(3, splitKeys.length);

    }

    @Test
    public void testCreateInstance_NoPrefix() {
        byte[][] splitKeys = TableConfig.parseSplitKeys(3, null, null);
        TableCreateDescriptor descriptor = new TableCreateDescriptor("mytable", splitKeys);
        assertEquals(2, descriptor.getSplitKeys().length);
    }

    @Test
    public void testCreateInstance_WithPrefix() {
        byte[][] inputSplitKeys = TableConfig.parseSplitKeys(3, null, "\\x01");
        TableCreateDescriptor descriptor = new TableCreateDescriptor("mytable", inputSplitKeys);
        byte[][] splitKeys = descriptor.getSplitKeys();
        assertEquals(2, descriptor.getSplitKeys().length);
        assertEquals(1, splitKeys[0][0]);
    }

}
