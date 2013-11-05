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
package org.lilyproject.process.test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.rest.RecordScannerMapBuilder;

public class RecordScannerMapTest {
    private Cache<String,RecordScanner> cache;

    @Before
    public void setup () {
        cache = RecordScannerMapBuilder.createRecordScannerMap(50, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testRecordScannerMapExpiration() throws Exception{
        RecordScanner scanner = new DummyRecordScanner();
        String id = "TTT";
        cache.put(id, scanner);
        Thread.sleep(100);
        RecordScanner returnScanner = cache.getIfPresent(id);
        Assert.assertNull(returnScanner);

    }

    @Test
    public void testRecordScannerMapAccessExpiration() throws Exception{
        RecordScanner scanner = new DummyRecordScanner();
        String id = "QQQ";
        cache.put(id, scanner);
        Thread.sleep(10);
        Assert.assertNotNull(cache.getIfPresent(id));
        Thread.sleep(100);
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
