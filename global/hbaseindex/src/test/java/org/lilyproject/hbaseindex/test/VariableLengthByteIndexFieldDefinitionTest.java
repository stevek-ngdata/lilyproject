package org.lilyproject.hbaseindex.test;

import org.junit.Test;

import java.math.BigInteger;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.lilyproject.hbaseindex.VariableLengthByteIndexFieldDefinition.decodeCustomizedPackedBcd;
import static org.lilyproject.hbaseindex.VariableLengthByteIndexFieldDefinition.encodeCustomizedPackedBcd;

/**
 * @author Jan Van Besien
 */
public class VariableLengthByteIndexFieldDefinitionTest {

    @Test
    public void testEncodeDecode() {
        BigInteger original;

        for (long i = 0; i < 1000000; i++) {
            original = BigInteger.valueOf(i);
            final byte[] encoded = encodeCustomizedPackedBcd(original);
            final BigInteger decoded = decodeCustomizedPackedBcd(encoded);
            assertEquals(original, decoded);
        }
    }

    @Test
    public void testEncode() {
        assertArrayEquals(new byte[]{0x00}, encodeCustomizedPackedBcd(BigInteger.valueOf(0)));
        assertArrayEquals(new byte[]{0x02}, encodeCustomizedPackedBcd(BigInteger.valueOf(1)));
        assertArrayEquals(new byte[]{0x04}, encodeCustomizedPackedBcd(BigInteger.valueOf(2)));
        assertArrayEquals(new byte[]{0x05}, encodeCustomizedPackedBcd(BigInteger.valueOf(3)));
        assertArrayEquals(new byte[]{0x07}, encodeCustomizedPackedBcd(BigInteger.valueOf(4)));
        assertArrayEquals(new byte[]{0x09}, encodeCustomizedPackedBcd(BigInteger.valueOf(5)));
        assertArrayEquals(new byte[]{0x0A}, encodeCustomizedPackedBcd(BigInteger.valueOf(6)));
        assertArrayEquals(new byte[]{0x0C}, encodeCustomizedPackedBcd(BigInteger.valueOf(7)));
        assertArrayEquals(new byte[]{0x0E}, encodeCustomizedPackedBcd(BigInteger.valueOf(8)));
        assertArrayEquals(new byte[]{0x0F}, encodeCustomizedPackedBcd(BigInteger.valueOf(9)));

        assertArrayEquals(new byte[]{0x24}, encodeCustomizedPackedBcd(BigInteger.valueOf(12)));
        assertArrayEquals(new byte[]{0x02, 0x45}, encodeCustomizedPackedBcd(BigInteger.valueOf(123)));
        assertArrayEquals(new byte[]{0x24, 0x57}, encodeCustomizedPackedBcd(BigInteger.valueOf(1234)));
    }

    @Test
    public void testDecode() {
        assertEquals(BigInteger.valueOf(0), decodeCustomizedPackedBcd(new byte[]{0x00}));
        assertEquals(BigInteger.valueOf(1), decodeCustomizedPackedBcd(new byte[]{0x02}));
        assertEquals(BigInteger.valueOf(2), decodeCustomizedPackedBcd(new byte[]{0x04}));
        assertEquals(BigInteger.valueOf(3), decodeCustomizedPackedBcd(new byte[]{0x05}));
        assertEquals(BigInteger.valueOf(4), decodeCustomizedPackedBcd(new byte[]{0x07}));
        assertEquals(BigInteger.valueOf(5), decodeCustomizedPackedBcd(new byte[]{0x09}));
        assertEquals(BigInteger.valueOf(6), decodeCustomizedPackedBcd(new byte[]{0x0A}));
        assertEquals(BigInteger.valueOf(7), decodeCustomizedPackedBcd(new byte[]{0x0C}));
        assertEquals(BigInteger.valueOf(8), decodeCustomizedPackedBcd(new byte[]{0x0E}));
        assertEquals(BigInteger.valueOf(9), decodeCustomizedPackedBcd(new byte[]{0x0F}));

        assertEquals(BigInteger.valueOf(12), decodeCustomizedPackedBcd(new byte[]{0x24}));
        assertEquals(BigInteger.valueOf(123), decodeCustomizedPackedBcd(new byte[]{0x02, 0x45}));
        assertEquals(BigInteger.valueOf(1234), decodeCustomizedPackedBcd(new byte[]{0x24, 0x57}));

        assertEquals(BigInteger.valueOf(123), decodeCustomizedPackedBcd(new byte[]{0x00, 0x02, 0x45}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidOne() {
        decodeCustomizedPackedBcd(new byte[]{0x01});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidThree() {
        decodeCustomizedPackedBcd(new byte[]{0x03});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidSix() {
        decodeCustomizedPackedBcd(new byte[]{0x06});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidEight() {
        decodeCustomizedPackedBcd(new byte[]{0x08});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidEleven() {
        decodeCustomizedPackedBcd(new byte[]{0x0B});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidThirteen() {
        decodeCustomizedPackedBcd(new byte[]{0x0D});
    }

}
