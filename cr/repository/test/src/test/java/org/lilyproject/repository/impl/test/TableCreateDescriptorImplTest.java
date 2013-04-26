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
import org.lilyproject.repository.api.TableManager.TableCreateDescriptor;
import org.lilyproject.repository.impl.TableCreateDescriptorImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TableCreateDescriptorImplTest {

    @Test
    public void testCreateInstance_OnlyName() {
        TableCreateDescriptor descriptor = TableCreateDescriptorImpl.createInstance("mytable");
        assertEquals("mytable", descriptor.getName());
        assertNull(descriptor.getSplitKeys());
    }


    @Test
    public void testCreateInstanceWithSplits_NoPrefix() {
        TableCreateDescriptor descriptor = TableCreateDescriptorImpl.createInstanceWithSplitKeys("mytable", null, "04,08");
        assertEquals(2, descriptor.getSplitKeys().length);
    }

    @Test
    public void testCreateInstanceWithSplits_WithPrefix() {
        TableCreateDescriptor descriptor = TableCreateDescriptorImpl.createInstanceWithSplitKeys("mytable", "\\x01", "04,08,12");
        byte[][] splitKeys = descriptor.getSplitKeys();
        assertEquals(3, splitKeys.length);

    }

    @Test
    public void testCreateInstance_NoPrefix() {
        TableCreateDescriptor descriptor = TableCreateDescriptorImpl.createInstance("mytable", null, 3);
        assertEquals(2, descriptor.getSplitKeys().length);
    }

    @Test
    public void testCreateInstance_WithPrefix() {
        TableCreateDescriptor descriptor = TableCreateDescriptorImpl.createInstance("mytable", "\\x01", 3);
        byte[][] splitKeys = descriptor.getSplitKeys();
        assertEquals(2, descriptor.getSplitKeys().length);
        assertEquals(1, splitKeys[0][0]);
    }

}
