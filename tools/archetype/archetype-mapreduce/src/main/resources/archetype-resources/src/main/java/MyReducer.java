package ${package};

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.lilyproject.client.LilyClient;
import org.lilyproject.mapreduce.LilyMapReduceUtil;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;

import java.io.IOException;

/**
 * Sample MapReduce reducer which writes its output to Lily. This is simply done using
 * LilyClient, like any other client application would do.
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private LilyClient lilyClient;
    private LRepository repository;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.lilyClient = LilyMapReduceUtil.getLilyClient(context.getConfiguration());
        try {
            this.repository = lilyClient.getDefaultRepository();
        } catch (RepositoryException e) {
            throw new RuntimeException("Failed to get repository", e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Closer.close(lilyClient);
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        try {
            // Create a record per word, with a field containing the count.
            // If there is already a record for this word, it will be overwritten.
            // The word is used a record id, we assume it only contains allowed characters.
            RecordId recordId = repository.getIdGenerator().newRecordId(key.toString());
            LTable table = repository.getDefaultTable();
            Record record = table.newRecord(recordId);
            record.setRecordType(new QName("mrsample", "Summary"));
            record.setField(new QName("mrsample", "wordcount"), sum);
            table.createOrUpdate(record);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
