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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.lilyproject.repository.bulk.BulkIngester;
import org.lilyproject.repository.bulk.LineMapper;
import org.lilyproject.repository.bulk.LineMappingContext;
import org.lilyproject.repository.bulk.jython.JythonLineMapper;

/**
 * Text line MapReduce mapper that sends input lines to a user-defined mapping function implemented in Python.
 */
public class LilyJythonMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    /** Config key for the mapper symbol name. */
    public static final String MAPPER_SYMBOL_NAME = "lilyproject.jython.mapper.symbol";

    /** Config key for the mapper Jython code. */
    public static final String MAPPER_CODE = "lilyproject.jython.mapper.code";

    /** Config key for Lily ZooKeeper connection string. */
    public static final String LILY_ZK_STRING = "lilyproject.zookeeper.connection";
    
    /** Config key for the name of the repository table to write to. */
    public static final String TABLE_NAME = "lilyproject.tablename";
    
    private LineMapper lineMapper;
    private BulkIngester bulkIngester;
    private LineMappingContext lineMappingContext;
    private MapReduceRecordWriter recordWriter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        lineMapper = new JythonLineMapper(conf.get(MAPPER_CODE), conf.get(MAPPER_SYMBOL_NAME));
        bulkIngester = BulkIngester.newBulkIngester(conf.get(LILY_ZK_STRING), 30000, conf.get(TABLE_NAME));
        recordWriter = new MapReduceRecordWriter(bulkIngester);
        lineMappingContext = new LineMappingContext(bulkIngester, recordWriter);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        recordWriter.setContext(context);
        lineMapper.mapLine(value.toString(), lineMappingContext);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        bulkIngester.close();
    }

}
