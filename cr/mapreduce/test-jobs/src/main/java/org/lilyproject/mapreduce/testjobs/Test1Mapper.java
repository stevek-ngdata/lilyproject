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
package org.lilyproject.mapreduce.testjobs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.lilyproject.mapreduce.RecordIdWritable;
import org.lilyproject.mapreduce.RecordMapper;
import org.lilyproject.mapreduce.RecordWritable;

public class Test1Mapper extends RecordMapper<Text, Text> {

    public void map(RecordIdWritable key, RecordWritable value, Context context)
            throws IOException, InterruptedException {

        Text keyOut = new Text();
        Text valueOut = new Text();

        // TODO do something useful
        keyOut.set("foo");
        valueOut.set("bar");

        context.write(keyOut, valueOut);
    }

}

