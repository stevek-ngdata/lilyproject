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
package org.lilyproject.hbaseindex.perftest;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.lilyproject.cli.OptionUtil;
import org.lilyproject.hbaseindex.Index;
import org.lilyproject.hbaseindex.IndexDefinition;
import org.lilyproject.hbaseindex.IndexEntry;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.hbaseindex.Query;
import org.lilyproject.hbaseindex.QueryResult;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.testclientfw.BaseTestTool;
import org.lilyproject.testclientfw.Words;
import org.lilyproject.util.Version;


public class HbaseIndexPerfTest extends BaseTestTool {
    private Index index;

    private IdGenerator idGenerator = new IdGeneratorImpl();

    private Option initialInsertOption;
    private Option initialInsertBatchOption;
    private Option loopsOption;

    private int initialInserts;
    private int initialInsertsBatchSize;
    private int loops;

    private int maxResults = 100;

    public static void main(String[] args) throws Exception {
        new HbaseIndexPerfTest().start(args);
    }

    @Override
    protected String getCmdName() {
        return "hbaseindex-perftest";
    }

    @Override
    protected String getVersion() {
        return Version.readVersion("org.lilyproject", "lily-hbaseindex-perftest");
    }

    @Override
    @SuppressWarnings("static-access")
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        initialInsertOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Initial index loading: number of entries to create")
                .withLongOpt("initial-entries")
                .create("e");
        options.add(initialInsertOption);

        initialInsertBatchOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Initial index loading: number of entries to add in one call to the index")
                .withLongOpt("initial-entries-batch")
                .create("b");
        options.add(initialInsertBatchOption);

        loopsOption = OptionBuilder
                .withArgName("amount")
                .hasArg()
                .withDescription("Number of loops to perform (each loop does multiple operations)")
                .withLongOpt("loops")
                .create("l");
        options.add(loopsOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        Configuration hbaseConf = getHBaseConf();

        IndexManager indexMgr = new IndexManager(hbaseConf);

        String indexName = "perftest1";

        IndexDefinition indexDef = new IndexDefinition(indexName);
        indexDef.addStringField("word");
        indexDef.addLongField("number");

        index = indexMgr.getIndex(indexDef);

        initialInserts = OptionUtil.getIntOption(cmd, initialInsertOption, 5000000);
        initialInsertsBatchSize = OptionUtil.getIntOption(cmd, initialInsertBatchOption, 300);
        loops = OptionUtil.getIntOption(cmd, loopsOption, 100000);

        System.out
                .println("Will insert " + initialInserts + " index entries in batches of " + initialInsertBatchOption);
        System.out.println("Will then perform " + loops + " tests on it");

        setupMetrics();

        doBulkLoad();

        doUsage();

        finishMetrics();

        return 0;
    }

    private void doBulkLoad() throws InterruptedException {
        startExecutor();

        int left = initialInserts;

        while (left > 0) {
            int amount = Math.min(left, initialInsertsBatchSize);
            left -= amount;
            executor.submit(new BulkInserter(amount));
        }

        stopExecutor();
    }

    private void doUsage() throws InterruptedException {
        startExecutor();


        for (int i = 0; i < loops; i++) {
            executor.submit(new SingleFieldEqualsQuery());
            executor.submit(new BulkInserter(1));
            executor.submit(new StringRangeQuery());
            executor.submit(new BulkInserter(5));
        }

        stopExecutor();
    }

    private class BulkInserter implements Runnable {
        private int amount;

        BulkInserter(int amount) {
            this.amount = amount;
        }

        @Override
        public void run() {
            try {
                List<IndexEntry> entries = new ArrayList<IndexEntry>(amount);

                for (int i = 0; i < amount; i++) {
                    IndexEntry entry = new IndexEntry(index.getDefinition());
                    entry.addField("word", Words.get());
                    entry.addField("number", (long) Math.floor(Math.random() * Long.MAX_VALUE));
                    entry.setIdentifier(idGenerator.newRecordId().toBytes());
                    entries.add(entry);
                }

                long before = System.nanoTime();
                index.addEntries(entries);
                double duration = System.nanoTime() - before;
                metrics.increment("Index insert in batch of " + amount, "I", amount, duration / 1e6d);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private class SingleFieldEqualsQuery implements Runnable {
        @Override
        public void run() {
            try {
                Query query = new Query();
                query.addEqualsCondition("word", Words.get());

                int resultCount = 0;

                long before = System.nanoTime();
                QueryResult result = index.performQuery(query);
                while (result.next() != null && resultCount < maxResults) {
                    resultCount++;
                }
                double duration = System.nanoTime() - before;
                metrics.increment("Single field query duration", "Q", duration / 1e6d);
                metrics.increment("Single field query # of results", resultCount);
                result.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private class StringRangeQuery implements Runnable {
        @Override
        public void run() {
            try {
                String word = "";
                while (word.length() < 3) {
                    word = Words.get();
                }

                String prefix = word.substring(0, 3);

                Query query = new Query();
                query.setRangeCondition("word", prefix, prefix, true, true);

                int resultCount = 0;

                long before = System.nanoTime();
                QueryResult result = index.performQuery(query);
                while (result.next() != null && resultCount < maxResults) {
                    resultCount++;
                }
                double duration = System.nanoTime() - before;
                metrics.increment("Str rng query duration", "Q", duration / 1e6d);
                metrics.increment("Str rng query # of results", resultCount);
                result.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}
