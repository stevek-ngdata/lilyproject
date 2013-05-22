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
package org.lilyproject.repository.impl;

import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseRepositoryStaticMethodTest {
    
    @Test
    public void testNextOcc_NullValue() {
        assertArrayEquals(Bytes.toBytes(1L), HBaseRepository.nextOcc(null));
    }
    
    @Test
    public void testNextOcc_NonNull() {
        byte[] oldOcc = Bytes.toBytes(1L);
        assertArrayEquals(Bytes.toBytes(2L), HBaseRepository.nextOcc(oldOcc));
        assertArrayEquals(Bytes.toBytes(1L), oldOcc);
    }
    
    @Test
    public void testNextOcc_LongOverflow() {
        byte[] oldOcc = Bytes.toBytes(Long.MAX_VALUE);
        assertArrayEquals(Bytes.toBytes(Long.MIN_VALUE), HBaseRepository.nextOcc(oldOcc));
        assertArrayEquals(Bytes.toBytes(Long.MAX_VALUE), oldOcc);
    }

}
