package org.lilyproject.mapreduce.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilyproject.mapreduce.RecordIdWritable;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecordIdWritableTest {
    @Test
    public void testComparisons() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();
        
        RecordIdWritable writable1 = new RecordIdWritable();
        RecordIdWritable writable2 = new RecordIdWritable();

        writable1.setRecordId(idGenerator.newRecordId("b"));
        writable2.setRecordId(idGenerator.newRecordId("b"));
        assertTrue(writable1.compareTo(writable2) == 0);

        writable2.setRecordId(idGenerator.newRecordId("c"));
        assertTrue(writable1.compareTo(writable2) < 0);

        writable2.setRecordId(idGenerator.newRecordId("a"));
        assertTrue(writable1.compareTo(writable2) > 0);

    }
    
    @Test
    public void testSerializationRoundTrip() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordIdWritable writable1 = new RecordIdWritable(idGenerator.newRecordId("foo"));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bos);
        writable1.write(out);
        
        System.out.println(Bytes.toStringBinary(bos.toByteArray()));
        
        // Verify the binary length
        assertEquals(1 /* vint length */ + 1 /* record id type byte */ + "foo".length(), bos.toByteArray().length);

        RecordIdWritable writable2 = new RecordIdWritable();
        writable2.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
        
        assertEquals(idGenerator.newRecordId("foo"), writable2.getRecordId());
    }
}
