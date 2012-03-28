package org.lilyproject.mapreduce.testjobs;

import org.apache.hadoop.io.Text;
import org.lilyproject.mapreduce.RecordIdWritable;
import org.lilyproject.mapreduce.RecordMapper;
import org.lilyproject.mapreduce.RecordWritable;

import java.io.IOException;
import java.util.Map;

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

