package org.lilyproject.indexer.batchbuild.hbasemr_patched;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;

public class TableMapReduceUtil {
    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @throws java.io.IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table, Scan scan,
        Class<? extends TableMapper> mapper,
        Class<? extends WritableComparable> outputKeyClass,
        Class<? extends Writable> outputValueClass, Job job)
    throws IOException {
      job.setInputFormatClass(TableInputFormat.class);
      if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
      if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
      job.setMapperClass(mapper);
      job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
      job.getConfiguration().set(TableInputFormat.SCAN,
        convertScanToString(scan));
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(out);
      scan.write(dos);
      return Base64.encodeBytes(out.toByteArray());
    }

    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param base64  The scan details.
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    static Scan convertStringToScan(String base64) throws IOException {
      ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
      DataInputStream dis = new DataInputStream(bis);
      Scan scan = new Scan();
      scan.readFields(dis);
      return scan;
    }
}
