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
package org.lilyproject.repository.bulk.serial;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.bulk.BulkIngester;
import org.lilyproject.repository.bulk.RecordWriter;
import org.lilyproject.util.concurrent.WaitPolicy;
import org.lilyproject.util.exception.ExceptionUtil;

/**
 * Import writer for bulk imports where the import runs as a single (multi-threaded) process writing directly to Lily.
 */
public class ThreadedRecordWriter implements RecordWriter {
    
    private Log log = LogFactory.getLog(getClass());
    
    private String lilyZk;
    private String repositoryTableName;
    private ThreadPoolExecutor executor;
    private AtomicLong recordsWritten = new AtomicLong();
    private AtomicLong writeFailures = new AtomicLong();
    private ThreadLocal<BulkIngester> threadLocalBulkIngesters;
    private List<BulkIngester> bulkIngesters;
    
    
    public ThreadedRecordWriter(String lilyZk, int numThreads, String repositoryTableName) {
        this.lilyZk = lilyZk;
        this.repositoryTableName = repositoryTableName;
        executor = new ThreadPoolExecutor(numThreads, numThreads, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(5));
        executor.setRejectedExecutionHandler(new WaitPolicy());
        threadLocalBulkIngesters  = new ThreadLocal<BulkIngester>();
        bulkIngesters = Collections.synchronizedList(Lists.<BulkIngester>newArrayList());
    }
    

    @Override
    public void write(final Record record) throws IOException, InterruptedException {
        executor.submit(new Runnable(){
            @Override
            public void run() {
                
                if (threadLocalBulkIngesters.get() == null) {
                    BulkIngester bulkIngester = BulkIngester.newBulkIngester(lilyZk, 30000, repositoryTableName);
                    bulkIngesters.add(bulkIngester);
                    threadLocalBulkIngesters.set(bulkIngester);
                }
                
                BulkIngester bulkIngester = threadLocalBulkIngesters.get();
                
                
                try {
                    bulkIngester.write(record);
                    recordsWritten.incrementAndGet();
                } catch (Exception e) {
                    ExceptionUtil.handleInterrupt(e);
                    log.error("Error writing record " + record, e);
                    writeFailures.incrementAndGet();
                }
            }});
    }
    
    @Override
    public void close() {
        this.executor.shutdown();
        boolean successfulFinish;
        try {
            successfulFinish = executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (!successfulFinish) {
            throw new RuntimeException("JSON import executor did not end successfully.");
        }
        for (BulkIngester bulkIngester : bulkIngesters) {
            try {
                bulkIngester.close();
            } catch (IOException e) {
                log.error("Error closing bulk ingester", e);
            }
        }
    }


    public long getNumWriteFailures() {
        return writeFailures.longValue();
    }
    
    @Override
    public long getNumRecords() {
        return recordsWritten.longValue();
    }

}
