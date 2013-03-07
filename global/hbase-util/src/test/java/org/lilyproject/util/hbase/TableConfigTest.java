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
package org.lilyproject.util.hbase;

import org.junit.Assert;
import org.junit.Test;

public class TableConfigTest {

    @Test
    public void testBinarySplitKeyParsing01() {
        final byte[][] splitKeys = new TableConfig(1, "\\x01", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(1, splitKeys[0].length);
        Assert.assertEquals(0x1, splitKeys[0][0]);
    }

    @Test
    public void testBinarySplitKeyParsing10() {
        final byte[][] splitKeys = new TableConfig(1, "\\x10", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(1, splitKeys[0].length);
        Assert.assertEquals(0x10, splitKeys[0][0]);
    }

    @Test
    public void testBinarySplitKeyParsing1010() {
        final byte[][] splitKeys = new TableConfig(1, "\\x10\\x10", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(2, splitKeys[0].length);
        Assert.assertEquals(0x10, splitKeys[0][0]);
        Assert.assertEquals(0x10, splitKeys[0][1]);
    }

    @Test
    public void testBinarySplitKeyParsing0F() {
        final byte[][] splitKeys = new TableConfig(1, "\\x0F", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(1, splitKeys[0].length);
        Assert.assertEquals(0xF, splitKeys[0][0]);
    }

    @Test
    public void testBinarySplitKeyParsingF0() {
        final byte[][] splitKeys = new TableConfig(1, "\\xF0", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(1, splitKeys[0].length);
        Assert.assertEquals(0xF0, readUnsigned(splitKeys[0][0]));
    }

    @Test
    public void testBinarySplitKeyParsingF0AB() {
        final byte[][] splitKeys = new TableConfig(1, "\\xF0\\xAB", new byte[]{}).getSplitKeys();
        Assert.assertEquals(1, splitKeys.length);
        Assert.assertEquals(2, splitKeys[0].length);
        Assert.assertEquals(0xF0, readUnsigned(splitKeys[0][0]));
        Assert.assertEquals(0xAB, readUnsigned(splitKeys[0][1]));
    }

    private static int readUnsigned(byte value) {
        return value & 0xFF;
    }
}
