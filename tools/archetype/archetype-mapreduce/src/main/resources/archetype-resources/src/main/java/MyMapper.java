package ${package};

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.lilyproject.mapreduce.RecordIdWritable;
import org.lilyproject.mapreduce.RecordMapper;
import org.lilyproject.mapreduce.RecordWritable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Sample MapReduce mapper which gets its input from Lily.
 */
public class MyMapper extends RecordMapper<Text, IntWritable> {
    private Text keyOut = new Text();
    private IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(RecordIdWritable recordIdWritable, RecordWritable recordWritable, Context context)
            throws IOException, InterruptedException {

        Record record = recordWritable.getRecord();
        String value = record.getField(new QName("mrsample", "text"));

        StringTokenizer tokenizer = new StringTokenizer(value);
        while (tokenizer.hasMoreTokens()) {
            keyOut.set(tokenizer.nextToken());
            context.write(keyOut, valueOut);
        }
    }
}
