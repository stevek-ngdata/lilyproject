package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * At the time of this writing, these are just some test to verify
 * some number byte sorting ideas.
 */
public class ByteNumberComparisonTest {
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

    /**
     * Converts an integer to a byte representation such that comparing the bytes
     * lexicographically compares the integer values.
     *
     * <p>Basically this is a matter of flipping the sign bit.
     */
    private byte[] toSortableBytes(int value) {
        byte[] bytes = Bytes.toBytes(value);

        if (value < 0) {
            bytes[0] = (byte)(bytes[0] & 0x7F);
        } else if (value >= 0) {
            bytes[0] = (byte)(bytes[0] | 0x80);            
        }

        return bytes;
    }

    /**
     * Converts a float to a byte representation such that comparing the bytes
     * lexicographically compares the float values.
     *
     * <p>A description of the IEEE float format used by Java can be found at
     * http://docs.sun.com/source/806-3568/ncg_math.html
     *
     * <p>Basically it consists of [sign bit][exponent bits][mantissa bits],
     * with the more significant bits to the left. For positive numbers, the
     * sorting will be automatically OK, but to get them bigger than the negatives
     * we flip the sign bit. Negative numbers are in sign-magnitude format (in
     * contrast to the two's complement representation of integers), to get
     * bigger magnitudes to become smaller we simply flip both the exponent and
     * mantissa bits.
     *
     * <p>Handling of infinity, subnormal numbers and both zeros (pos & neg) should
     * be fine, handling of not-a-number is undefined.
     */
    private byte[] toSortableBytes(float value) {
        byte[] bytes = Bytes.toBytes(value);

        // Check the leftmost bit to determine if the value is negative
        int test = (bytes[0] >>> 7) & 0x01;
        if (test == 1) {
            // Negative numbers: flip all bits: sign, exponent and mantissa
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte)(bytes[i] ^ 0xFF);
            }
        } else {
            // Positive numbers: flip the sign bit
            bytes[0] = (byte)(bytes[0] | 0x80);
        }

        return bytes;
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
