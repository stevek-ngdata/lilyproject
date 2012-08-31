About this MapReduce job
========================

This is a template project for writing your own Lily MapReduce job.

It contains a sample mapper which uses the LilyInputFormat to read
data from Lily. It also contains a sample reducer, which writes its
output to Lily using the LilyClient.

If you don't need a reducer and want to write to Lily directly
from your mapper, just copy over the relevant parts from the reducer
sample.

As part of the build, a job-jar is created which contains all
required dependencies in the lib subdir.

To run this example:

 - have a Lily stack running, e.g. using launch-test-lily

 - import testdata.json sample data:
   lily-import testdata.json

 - start the MapReduce job using:
   cd /to/hadoop
   ./bin/hadoop jar my-lily-mrjob/target/my-lily-mrjob-1.0-SNAPSHOT-mapreduce-job.jar com.mycompany.MyJob

   (this assumes JobTracker/Namenode/ZooKeeper is running on localhost,
   see MyJob code to adjust)

 - if the job has run correctly, you can run lily-scan-records to
   check the output produced by the reducer:
   lily-scan-records -p --record-type {mrsample}Summary

Setting up Hadoop with CHD4: 
  - Download hadoop-2.0.0-mr1 for the MRv1 variant
  
  - Delete the avro libs under $HADOOP_HOME/lib/avro*
  
  - Set HADOOP_CLASSPATH 
    To do this grab the classpath found in a lily application startup script e.g. lily-scan-records
    
  - Now you are ready to run the mapreduce jar under CDH4

   
