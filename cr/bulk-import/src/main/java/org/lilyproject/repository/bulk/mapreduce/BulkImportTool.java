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
package org.lilyproject.repository.bulk.mapreduce;

import java.io.File;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.python.google.common.io.Files;

/**
 * MapReduce-based import tool that makes use of Jython-based text line mapping.
 */
public class BulkImportTool extends Configured implements Tool {

    private static final String HFILE_PATH = "lilyproject.bulkimport.hfilepath";

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input file> <python mapper file> <python mapper symbol>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String inputPath = args[0];
        String pythonFilePath = args[1];
        String pythonSymbol = args[2];

        Configuration conf = getConf();

        // TODO This needs to be a parameter
        conf.set(LilyJythonMapper.LILY_ZK_STRING, "localhost:2181");
        conf.set(LilyJythonMapper.MAPPER_CODE, Files.toString(new File(pythonFilePath), Charsets.UTF_8));
        conf.set(LilyJythonMapper.MAPPER_SYMBOL_NAME, pythonSymbol);

        Job job = new Job(conf);

        Path tmpDir = new Path("/tmp/lily-" + UUID.randomUUID());

        job.setJarByClass(BulkImportTool.class);
        job.setMapperClass(LilyJythonMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));
        HFileOutputFormat.setOutputPath(job, tmpDir);
        conf.set(HFILE_PATH, tmpDir.toUri().toString());

        HTable recordTable = new HTable(conf, LilyHBaseSchema.Table.RECORD.bytes);
        HFileOutputFormat.configureIncrementalLoad(job, recordTable);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Job failed");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new BulkImportTool(), args);
        if (status != 0) {
            System.exit(status);
        }
        SchemaMetrics.configureGlobally(conf);
        status = ToolRunner.run(new LoadIncrementalHFiles(conf),
                new String[] { conf.get(HFILE_PATH), Table.RECORD.name });
        FileSystem.get(conf).delete(new Path(new URI(conf.get(HFILE_PATH))), true);
        System.exit(status);
    }

}
