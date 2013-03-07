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
package org.lilyproject.bytes.impl.test;

import java.util.Random;

import org.junit.Assert;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;

import junit.framework.TestCase;

/**
 * Test the encodings used in DataInputImpl and DataOutputImpl
 *
 * <p>The code to generate the randomUnicodeString has been based on code from Lucene class :
 * <code>org.apache.lucene.util._TestUtil.java</code>
 */
public class TestDataInputOutput extends TestCase {
    private Random random = new Random(System.currentTimeMillis());

    /** start and end are BOTH inclusive */
    public int nextInt(int start, int end) {
      return start + random.nextInt(end-start+1);
    }

    /** Returns random string, including full unicode range. */
    public String randomUnicodeString() {
      return randomUnicodeString(20);
    }

    /**
     * Returns a random string up to a certain length.
     */
    public String randomUnicodeString(int maxLength) {
      final int end = random.nextInt(maxLength);
      if (end == 0) {
        // allow 0 length
        return "";
      }
      final char[] buffer = new char[end];
      randomFixedLengthUnicodeString(buffer, 0, buffer.length);
      return new String(buffer, 0, end);
    }

    /**
     * Fills provided char[] with valid random unicode code
     * unit sequence.
     */
    public void randomFixedLengthUnicodeString(char[] chars, int offset, int length) {
      int i = offset;
      final int end = offset + length;
      while(i < end) {
        final int t = random.nextInt(5);
        if (0 == t && i < length - 1) {
          // Make a surrogate pair
          // High surrogate
          chars[i++] = (char) nextInt(0xd800, 0xdbff);
          // Low surrogate
          chars[i++] = (char) nextInt(0xdc00, 0xdfff);
        } else if (t <= 1) {
          chars[i++] = (char) random.nextInt(0x80);
        } else if (2 == t) {
          chars[i++] = (char) nextInt(0x80, 0x800);
        } else if (3 == t) {
          chars[i++] = (char) nextInt(0x800, 0xd7ff);
        } else if (4 == t) {
          chars[i++] = (char) nextInt(0xe000, 0xffff);
        }
      }
    }

    public void testRandomString() {
        for (int i = 0; i < 1000; i++) {
            String string = randomUnicodeString();
            DataOutput dataOutput = new DataOutputImpl();
            dataOutput.writeUTF(string);
            byte[] bytes = dataOutput.toByteArray();
            DataInput dataInput = new DataInputImpl(bytes);
            String result = dataInput.readUTF();
            Assert.assertEquals(string, result);
        }
    }

    public void testRandomVString() {
        for (int i = 0; i < 1000; i++) {
            String string = randomUnicodeString();
            DataOutput dataOutput = new DataOutputImpl();
            dataOutput.writeVUTF(string);
            byte[] bytes = dataOutput.toByteArray();
            DataInput dataInput = new DataInputImpl(bytes);
            String result = dataInput.readVUTF();
            Assert.assertEquals(string, result);
        }
    }

    public void testAllCombinations() {
        char[] chars = new char[6];
        int i = 0;
        // Make a surrogate pair
        // High surrogate
        chars[i++] = (char) nextInt(0xd800, 0xdbff);
        // Low surrogate
        chars[i++] = (char) nextInt(0xdc00, 0xdfff);
        // 1
        chars[i++] = (char) random.nextInt(0x80);
        // 2
        chars[i++] = (char) nextInt(0x80, 0x800);
        // 3
        chars[i++] = (char) nextInt(0x800, 0xd7ff);
        // 4
        chars[i++] = (char) nextInt(0xe000, 0xffff);
        String string = new String(chars);
        DataOutputImpl dataOutputImpl = new DataOutputImpl();
        dataOutputImpl.writeUTF(string);
        byte[] data = dataOutputImpl.toByteArray();
        DataInputImpl dataInputImpl = new DataInputImpl(data);
        String readUTF = dataInputImpl.readUTF();
        Assert.assertEquals(string, readUTF);
    }

    public void testHyphen() {
        DataOutputImpl dataOutputImpl = new DataOutputImpl();
        String string = "Mary Shelley (30 August 1797 – 1 February 1851) was a British novelist"; // Note, the hyphen is not just a minus sign
        dataOutputImpl.writeUTF(string);
        byte[] data = dataOutputImpl.toByteArray();
        DataInputImpl dataInputImpl = new DataInputImpl(data);
        String readUTF = dataInputImpl.readUTF();
        Assert.assertEquals(string, readUTF);
    }

    public void testRussian() {
        DataOutputImpl dataOutputImpl = new DataOutputImpl();
        String string = "ТЕСТ"; // Note, these are russian characters
        dataOutputImpl.writeUTF(string);
        byte[] data = dataOutputImpl.toByteArray();
        DataInputImpl dataInputImpl = new DataInputImpl(data);
        String readUTF = dataInputImpl.readUTF();
        Assert.assertEquals(string, readUTF);
    }

    public void testAllTypes() {
        DataOutput dataOutput = new DataOutputImpl();
        boolean b = random.nextBoolean();
        dataOutput.writeBoolean(b);
        byte[] bytes = new byte[10];
        random.nextBytes(bytes);
        dataOutput.writeByte(bytes[0]);
        dataOutput.writeBytes(bytes);
        double d = random.nextDouble();
        dataOutput.writeDouble(d);
        float f = random.nextFloat();
        dataOutput.writeFloat(f);
        int i = random.nextInt();
        dataOutput.writeInt(i);
        long l = random.nextLong();
        dataOutput.writeLong(l);
        short s = (short)4;
        dataOutput.writeShort(s);
        String string = randomUnicodeString();
        dataOutput.writeUTF(string);
        dataOutput.writeVInt(Math.abs(i));
        dataOutput.writeVLong(Math.abs(l));

        byte[] data = dataOutput.toByteArray();
        DataInput dataInput = new DataInputImpl(data);
        Assert.assertEquals(b, dataInput.readBoolean());
        Assert.assertEquals(bytes[0], dataInput.readByte());
        Assert.assertArrayEquals(bytes, dataInput.readBytes(10));
        Assert.assertEquals(d, dataInput.readDouble(), 0.0001);
        Assert.assertEquals(f, dataInput.readFloat(), 0.0001);
        Assert.assertEquals(i, dataInput.readInt());
        Assert.assertEquals(l, dataInput.readLong());
        Assert.assertEquals(s, dataInput.readShort());
        Assert.assertEquals(string, dataInput.readUTF());
        Assert.assertEquals(Math.abs(i), dataInput.readVInt());
        Assert.assertEquals(Math.abs(l), dataInput.readVLong());
    }
}
