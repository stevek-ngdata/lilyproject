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
package org.lilyproject.hbaseindex;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Various tests that check whether the comparison of the binary representations
 * of values gives the expected result.
 *
 * Actually we are testing Orderly here, which is the underlying library used for encoding/decoding index row keys.
 */
public class ByteComparisonTest {
    @Test
    public void testSignedIntegerCompare() throws Exception {
        int[] testNumbers = {
                Integer.MIN_VALUE,
                -1000,
                0,
                1000,
                Integer.MAX_VALUE};

        for (int n1 : testNumbers) {
            byte[] n1Bytes = toSortableBytes(n1);
            for (int n2 : testNumbers) {
                byte[] n2Bytes = toSortableBytes(n2);
                int cmp = Bytes.compareTo(n1Bytes, n2Bytes);
                if (cmp == 0) {
                    if (n1 != n2) {
                        System.out.println(n1 + " = " + n2);
                        assertTrue(n1 == n2);
                    }
                } else if (cmp < 0) {
                    if (!(n1 < n2)) {
                        System.out.println(n1 + " < " + n2);
                        assertTrue(n1 < n2);
                    }
                } else if (cmp > 0) {
                    if (!(n1 > n2)) {
                        System.out.println(n1 + " > " + n2);
                        assertTrue(n1 > n2);
                    }
                }
            }
        }
    }

    @Test
    public void testSignedFloatCompare() throws Exception {
        float[] testNumbers = {
                Float.NEGATIVE_INFINITY,
                Float.MIN_VALUE,
                -Float.MIN_VALUE / 2,
                -1000f,
                /* this is intended to be a subnormal number */
                -0.000000000000000000000000000000000000000000001f,
                -0f,
                0f,
                0.000000000000000000000000000000000000000000001f,
                Float.MIN_NORMAL,
                1000f,
                Float.MAX_VALUE / 2,
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY};

        for (float n1 : testNumbers) {
            byte[] n1Bytes = toSortableBytes(n1);

            for (float n2 : testNumbers) {
                byte[] n2Bytes = toSortableBytes(n2);
                int cmp = Bytes.compareTo(n1Bytes, n2Bytes);
                if (cmp == 0) {
                    assertTrue(n1 == n2);
                } else if (cmp < 0) {
                    if (!(n1 < n2) && !(n1 == -0f && n2 == 0f)) {
                        System.out.println(n1 + " > " + n2);
                        assertTrue(n1 < n2);
                    }
                } else if (cmp > 0) {
                    if (!(n1 > n2) && !(n1 == 0f && n2 == -0f)) {
                        System.out.println(n1 + " > " + n2);
                        assertTrue(n1 > n2);
                    }
                }
            }
        }
    }

    @Test
    public void testUtf8StringCompare() throws Exception {
        StringIndexFieldDefinition fieldDef = new StringIndexFieldDefinition("foobar");

        byte[] string1 = fieldDef.asRowKey().serialize(Bytes.toBytes("\u00EAtre"));

        byte[] string2 = fieldDef.asRowKey().serialize(Bytes.toBytes("heureux"));

        assertTrue(Bytes.compareTo(string1, string2) > 0);
    }

    private byte[] toSortableBytes(int value) throws IOException {
        IntegerIndexFieldDefinition fieldDef = new IntegerIndexFieldDefinition("foobar");
        return fieldDef.asRowKey().serialize(value);
    }

    private byte[] toSortableBytes(float value) throws IOException {
        FloatIndexFieldDefinition fieldDef = new FloatIndexFieldDefinition("foobar");
        return fieldDef.asRowKey().serialize(value);
    }

    private byte[] toSortableBytes(BigDecimal value) throws IOException {
        DecimalIndexFieldDefinition fieldDef = new DecimalIndexFieldDefinition("foobar");
        return fieldDef.asRowKey().serialize(value);
    }

    @Test
    public void testDecimalCompare() throws Exception {
        String[] testNumbers = {"-99999999999999999999999999999999999999999999999999999999999999999999999999999999",
                "-10.000000000000000000000000000000000000000000000000000000000000000000000000000000002",
                "-10.000000000000000000000000000000000000000000000000000000000000000000000000000000001",
                "0",
                "5.5",
                "55.5",
                "0.1E16383"};

        BigDecimal[] testDecimals = new BigDecimal[testNumbers.length];
        for (int i = 0; i < testDecimals.length; i++) {
            testDecimals[i] = new BigDecimal(testNumbers[i]);
        }

        for (BigDecimal d1 : testDecimals) {
            for (BigDecimal d2 : testDecimals) {
                byte[] b1 = toSortableBytes(d1);
                byte[] b2 = toSortableBytes(d2);
                int cmp = Bytes.compareTo(b1, b2);
                if (cmp == 0) {
                    if (!d1.equals(d2)) {
                        System.out.println(d1 + " == " + d2);
                        fail();
                    }
                } else if (cmp < 0) {
                    if (!(d1.compareTo(d2) < 0)) {
                        System.out.println(d1 + " < " + d2);
                        fail();
                    }
                } else if (cmp > 0) {
                    if (!(d1.compareTo(d2) > 0)) {
                        System.out.println(d1 + " > " + d2);
                        fail();
                    }
                }
            }
        }
    }

    /**
     * Code to print out a complete bit representation of a byte,
     * including leading zeros.
     *
     * <p>Copied from
     * http://manniwood.wordpress.com/2009/10/21/javas-long-tobinarystringlong-l-come-on-guys/
     */
    private String toBinaryString(byte val) {
        StringBuilder sb = new StringBuilder(8);
        for (int i = 7; i >= 0; i--) {
            sb.append((testBit(val, i) == 0) ? '0' : '1');
        }
        return sb.toString();
    }

    byte testBit(byte val, int bitToTest) {
        val >>= bitToTest;
        val &= 1;
        return val;
    }

    private String toBinaryString(byte[] values) {
        StringBuilder sb = new StringBuilder(values.length * 8);
        for (byte value : values) {
            sb.append(toBinaryString(value));
        }
        return sb.toString();
    }
}
