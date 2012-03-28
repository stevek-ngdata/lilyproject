package org.lilyproject.process.test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.rest.RecordScannerMapBuilder;

import com.google.common.cache.Cache;

public class RecordScannerMapTest {
    private Cache<String,RecordScanner> cache;
    
    @Before
    public void setup () {
        cache = RecordScannerMapBuilder.createRecordScannerMap(10, TimeUnit.MILLISECONDS);
    }
    
    
    @Test
    public void testRecordScannerMapExpiration() throws Exception{
        RecordScanner scanner = new DummyRecordScanner();
        String id = "TTT";
        cache.put(id, scanner);
        Thread.currentThread().sleep(11);
        RecordScanner returnScanner = cache.getIfPresent(id);
        Assert.assertNull(returnScanner);
        
    }
    
    @Test
    public void testRecordScannerMapAccessExpiration() throws Exception{
        RecordScanner scanner = new DummyRecordScanner();
        String id = "QQQ";
        cache.put(id, scanner);
        Thread.currentThread().sleep(5);
        Assert.assertNotNull(cache.getIfPresent(id));
        // should still be here after 1.3s
        Thread.currentThread().sleep(8);
        Assert.assertNotNull(cache.getIfPresent(id));
        Thread.currentThread().sleep(11);
        Assert.assertNull(cache.getIfPresent(id));
    }
    
    private class DummyRecordScanner implements RecordScanner {
        // This is a dummy, it does not need to do anything; 

        @Override
        public Iterator<Record> iterator() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Record next() throws RepositoryException, InterruptedException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            
        }
        
    }

}
