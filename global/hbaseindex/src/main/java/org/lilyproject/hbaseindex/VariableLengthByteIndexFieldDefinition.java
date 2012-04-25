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

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.util.ArgumentValidator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;

/**
 * This kind of field allows to store arbitrary byte arrays in the index key, the ideal fallback the case none of the
 * other types suite your needs. It differs from ByteIndexFieldDefinition in that it supports variable length bytes. It
 * does this by interpreting the byte array as an unsigned decimal number and then applying (customized) "binary coded
 * decimal" encoding (http://en.wikipedia.org/wiki/Binary-coded_decimal).
 */
public class VariableLengthByteIndexFieldDefinition extends IndexFieldDefinition {

    // About the encoding scheme:
    //
    // variable length byte arrays can not be written simply as the original byte array, because this would leave us
    // without any usable byte (or byte sequence) to mark the end of the byte array. We can also not simply write
    // the length of the array in a fixed number of bytes in the beginning of the output, because this would mean
    // the rowkeys can no longer be sorted and "prefix searched". This impacts things like splitting tables over
    // regions etc.
    //
    // Therefor we use a variant of packed binary coded decimal (BCD). We start by interpreting the byte array
    // as an unsigned variable length integer number represented in decimal format (using only digits 0 - 9).
    // Packed BCD encodes each digit (0 - 9) into 4 bits (binary 0000 - 1001) and "packs" two times 4 of these bits
    // into one byte. This is almost what we want. There are two remaining problems:
    // - half of the resulting values (when the input is random) will start with 4 zero bits (for uneven number of
    //   digits. We solve this by writing the bytes in reversed order.
    // - BCD only uses the byte range (binary) 00000000 - 10011001. This results in an uneven distribution of values
    //   over the whole possible range, resulting in problems with splitting tables over regions based on predefined
    //   byte ranges for each region. To solve this, we modified BCD with a mapping from digits to 4 bit values
    //   which uses the range more evenly.
    //
    // The mapping is:
    // digit 0 = binary 0000 (0x00)
    // digit 1 = binary 0010 (0x02)
    // digit 2 = binary 0100 (0x04)
    // digit 3 = binary 0101 (0x05)
    // digit 4 = binary 0111 (0x07)
    // digit 5 = binary 1001 (0x09)
    // digit 6 = binary 1010 (0x0A)
    // digit 7 = binary 1100 (0X0C)
    // digit 8 = binary 1110 (0x0E)
    // digit 9 = binary 1111 (0x0F)
    //
    // This also means that a number of values are "available" as terminator, we've chosen 0001. Two of these values
    // together form a terminator byte, thus binary 00010001 (or 0x11).

    /**
     * An optional number of bytes at the beginning of the byte array which have a fixed length and will always be
     * present.
     */
    private int fixedPartLength = 0;

    /**
     * As end marker we use a byte which will never be a valid packed "customized" BCD byte.
     */
    private static final byte END_MARKER = 0x11;

    public VariableLengthByteIndexFieldDefinition(String name, int fixedPartLength) {
        super(name, IndexValueType.BYTES);
        if (fixedPartLength < 0)
            throw new IllegalArgumentException("fixed part length should be positive");

        this.fixedPartLength = fixedPartLength;
    }

    public VariableLengthByteIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.BYTES, jsonObject);

        if (jsonObject.get("fixedPartLength") != null)
            this.fixedPartLength = jsonObject.get("fixedPartLength").getIntValue();

    }

    @Override
    public byte[] toBytes(Object value) {
        byte[] byteValue = (byte[]) value;

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(os);
        try {
            // write fixed length part
            dos.write(byteValue, 0, fixedPartLength);
            // write variable length part
            byte[] variableLengthPart = new byte[byteValue.length - fixedPartLength];
            System.arraycopy(byteValue, fixedPartLength, variableLengthPart, 0, variableLengthPart.length);
            final byte[] encoded = encodeCustomizedPackedBcd(new BigInteger(1, variableLengthPart));
            reverse(encoded);
            dos.write(encoded);
        } catch (IOException e) {
            // ignore: ByteArrayOutputStream doesn't throw IOException
        } // no need to close ByteArrayOutputStream

        return os.toByteArray();
    }

    private static void reverse(byte[] array) {
        int i = 0;
        int j = array.length - 1;
        byte tmp;
        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode object = super.toJson();
        object.put("fixedPartLength", fixedPartLength);
        return object;
    }

    @Override
    public int getLength() {
        return -1; // variable length
    }

    @Override
    public byte[] getEndOfFieldMarker() {
        return new byte[]{END_MARKER};
    }

    /**
     * Encodes a positive BigInteger into a "customized packed binary coded decimal" byte array.
     *
     * @param value value to be encoded
     * @return the customized packed BCD encoded byte array
     */
    public static byte[] encodeCustomizedPackedBcd(BigInteger value) {
        ArgumentValidator.notNull(value, "value");
        if (value.signum() < 0)
            throw new IllegalArgumentException("can not encode negative numbers");

        final char[] digits = value.toString(10).toCharArray();
        final byte[] bcd = digitsToCustomizedBcd(digits);
        final byte[] packedBcd = bcdToPackedBcd(bcd);

        return packedBcd;
    }

    private static byte[] CUSTOMIZED_BCD_ENCODING_LOOKUP_TABLE = new byte[]{0, 2, 4, 5, 7, 9, 10, 12, 14, 15};

    // note that the value -1 means invalid
    private static byte[] CUSTOMIZED_BCD_DECODING_LOOKUP_TABLE =
            new byte[]{0, -1, 1, -1, 2, 3, -1, 4, -1, 5, 6, -1, 7, -1, 8, 9};

    private static byte[] digitsToCustomizedBcd(char[] digits) {
        final byte[] bcd = new byte[digits.length];
        for (int i = 0; i < digits.length; i++) {
            bcd[i] = CUSTOMIZED_BCD_ENCODING_LOOKUP_TABLE[Character.digit(digits[i], 10)];
        }
        return bcd;
    }

    private static byte[] bcdToPackedBcd(byte[] bcd) {
        final byte[] packedBcd = new byte[(bcd.length >> 1) + (bcd.length & 1)];
        for (int i = alignFirstByte(bcd, packedBcd), j = i; j < bcd.length; j += 2) {
            packedBcd[i] = (byte) (bcd[j] << 4);
            packedBcd[i++] |= bcd[j + 1];
        }
        return packedBcd;
    }

    private static int alignFirstByte(byte[] bcd, byte[] packedBcd) {
        if ((bcd.length & 1) == 1) {
            packedBcd[0] = bcd[0];
        }
        return (bcd.length & 1);
    }

    /**
     * Decodes a "customized packed binary coded decimal" byte array into a BigInteger.
     *
     * @param bytes the customized packed BCD encoded byte array
     * @return The decoded value
     */
    public static BigInteger decodeCustomizedPackedBcd(byte[] bytes) {
        final StringBuilder buf = new StringBuilder(bytes.length * 2);

        for (int i = 0; i < bytes.length; i++) {
            final byte decodedFirst4Bits = CUSTOMIZED_BCD_DECODING_LOOKUP_TABLE[((bytes[i] & 0xf0) >> 4)];
            checkValidCustomizedBcdDigit(decodedFirst4Bits);
            buf.append((char) (decodedFirst4Bits + '0'));

            if ((i != bytes.length)) {
                final byte decodedLast4Bits = CUSTOMIZED_BCD_DECODING_LOOKUP_TABLE[(bytes[i] & 0x0f)];
                checkValidCustomizedBcdDigit(decodedLast4Bits);
                buf.append((char) (decodedLast4Bits + '0'));
            }
        }

        return new BigInteger(buf.toString(), 10);
    }

    private static void checkValidCustomizedBcdDigit(byte bcdDigit) {
        if (bcdDigit == -1)
            throw new IllegalArgumentException("invalid customized packed BCD format detected");
    }
}
