package ${package};

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.mapreduce.LilyInputFormat;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkConnectException;

import java.io.IOException;

/**
 * Sample MapReduce reducer which writes its output to Lily. This is simply done using
 * LilyClient, like any other client application would do.
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private LilyClient lilyClient;
    private Repository repository;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        String zkConnectString = context.getConfiguration().get(LilyInputFormat.ZK_CONNECT_STRING);
        try {
            lilyClient = new LilyClient(zkConnectString, 30000);
            repository = lilyClient.getRepository();
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        Closer.close(lilyClient);
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
            // This only works correctly because we have one reducer.
            // We assume the words don't contain any invalid record id characters.
            RecordId recordId = repository.getIdGenerator().newRecordId(key.toString());
            Record record = repository.newRecord(recordId);
            record.setRecordType(new QName("mrsample", "Summary"));
            record.setField(new QName("mrsample", "wordcount"), sum);
            repository.createOrUpdate(record);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
