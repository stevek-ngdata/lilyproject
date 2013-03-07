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
package org.lilyproject.util.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.format.DateTimeFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.hadooptestfw.HBaseProxy;

public class HBaseTableFactoryTest {

    private static HBaseProxy HBASE_PROXY;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        HBASE_PROXY = new HBaseProxy();
        HBASE_PROXY.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        HBASE_PROXY.stop();
    }

    @Test
    public void testCreateTableConcurrently() {
        final Configuration configuration = HBASE_PROXY.getConf();
        final HBaseTableFactory tableFactory = new HBaseTableFactoryImpl(configuration);

        String tableName = "table-" + timestamp();
        final HTableDescriptor descr = newTableDescriptor(tableName);

        ExecutorService svc = Executors.newFixedThreadPool(10);
        List<Future<HTableInterface>> futures = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            futures.add(svc.submit(new Callable<HTableInterface>() {
                @Override
                public HTableInterface call() throws IOException, InterruptedException {
                    return tableFactory.getTable(descr, true);
                }
            }));
        }

        List<Throwable> throwables = Lists.newArrayList();
        for (Future<HTableInterface> f: futures) {
            try {
                f.get();
            } catch (Throwable t) {
                throwables.add(t);
                t.printStackTrace();
            }
        }

        Assert.assertEquals("Concurrent getTable(name,create=true) calls for the same table should not fail", Collections.EMPTY_LIST, throwables);

    }

    private String timestamp() {
        return DateTimeFormat.forPattern("yyyyMMdd-hhmmss").print(System.currentTimeMillis());
    }

    private HTableDescriptor newTableDescriptor(String tableName) {
        HTableDescriptor tableDescr = new HTableDescriptor(tableName);
        HColumnDescriptor family =
                new HColumnDescriptor(Bytes.toBytes("mycolumn"), 1, HColumnDescriptor.DEFAULT_COMPRESSION,
                        HColumnDescriptor.DEFAULT_IN_MEMORY, HColumnDescriptor.DEFAULT_BLOCKCACHE,
                        HColumnDescriptor.DEFAULT_BLOCKSIZE, HColumnDescriptor.DEFAULT_TTL,
                        HColumnDescriptor.DEFAULT_BLOOMFILTER, HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
        tableDescr.addFamily(family);

        tableDescr.setValue(Bytes.toBytes("foo"), Bytes.toBytes("bar"));

        return tableDescr;
    }

}
