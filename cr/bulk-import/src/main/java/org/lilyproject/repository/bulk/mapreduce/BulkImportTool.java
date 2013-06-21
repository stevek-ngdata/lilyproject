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

import com.google.common.base.Charsets;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lilyproject.repository.bulk.AbstractBulkImportCliTool;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.python.google.common.io.Files;

/**
 * MapReduce-based import tool that makes use of Jython-based text line mapping.
 */
public class BulkImportTool extends AbstractBulkImportCliTool implements Tool {

    private static final String HFILE_PATH = "lilyproject.bulkimport.hfilepath";

    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        start(args);
        return 0;
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    

    @Override
    protected String getCmdName() {
        return "lily-bulk-import";
    }
    
    private String formatJobName() {
        return String.format("%s: %s %s %s", getCmdName(), pythonMapperPath, pythonSymbol, inputPath);
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int status = super.run(cmd);

        if (status != 0) {
            return status;
        }
        
        String tableName = Table.RECORD.name;
        if (outputTable != null) {
            tableName = outputTable;
        }

        conf.set("hbase.zookeeper.quorum", zkConnectionString);
        conf.set(LilyJythonMapper.LILY_ZK_STRING, zkConnectionString);
        conf.set(LilyJythonMapper.MAPPER_CODE, Files.toString(new File(pythonMapperPath), Charsets.UTF_8));
        conf.set(LilyJythonMapper.MAPPER_SYMBOL_NAME, pythonSymbol);
        conf.set(LilyJythonMapper.TABLE_NAME, tableName);

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
        job.setJobName(formatJobName());
        TextInputFormat.addInputPath(job, new Path(inputPath));
        HFileOutputFormat.setOutputPath(job, tmpDir);
        conf.set(HFILE_PATH, tmpDir.toUri().toString());

        HTable recordTable = new HTable(conf, tableName);
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
                new String[] { conf.get(HFILE_PATH), conf.get(LilyJythonMapper.TABLE_NAME) });
        FileSystem.get(conf).delete(new Path(new URI(conf.get(HFILE_PATH))), true);
        System.exit(status);
    }

}
